from json import loads, dumps
from threading import Thread, Event
from logging.handlers import BufferingHandler
from traceback import format_exception as fmtex
from elasticsearch import helpers as eshelpers
from elasticsearch import Elasticsearch, Urllib3HttpConnection
from .utils import (AuthType, IndexNameFreq, ESSerializer,
                    INDEX_NAME_FUNC_DICT, _get_es_datetime_str)


class ESHandler(BufferingHandler):
    def __init__(self,
                 *,
                 hosts=[{'host': 'localhost', 'port': 9200}],
                 auth_type=AuthType.NO_AUTH,
                 auth_details=None,
                 use_ssl=False,
                 verify_ssl=True,
                 buffer_size=1000,
                 connection=Urllib3HttpConnection,
                 connection_timeout=10,
                 flush_frequency_in_sec=1,
                 es_client=None,
                 es_index_name=None,
                 es_index_name_frequency=IndexNameFreq.DAILY,
                 es_additional_fields={},
                 raise_on_exceptions=False,
                 backup_filepath='backup.log'):
        if not isinstance(es_index_name, str):
            raise TypeError('es_index_name must be a string')

        if es_client is not None and not isinstance(es_client, Elasticsearch):
            raise TypeError('es_client must be Elasticsearch instance or None')

        BufferingHandler.__init__(self, buffer_size)

        self.hosts = hosts
        self.auth_details = auth_details
        self.auth_type = self.__get_auth_details(auth_type)

        self.use_ssl = use_ssl
        self.verify_certs = verify_ssl

        self.es_idx_name = es_index_name
        self.es_add_fields = es_additional_fields

        self._client = es_client
        self._connection_class = connection
        self._connection_timeout = connection_timeout
        self._flush_frequency = flush_frequency_in_sec
        self._idx_name_func = self.__get_idx_name_func(es_index_name_frequency)
        self._raise_on_exceptions = raise_on_exceptions

        self._backup_file = self.__backup_restore(backup_filepath)

        self.__stop_event = Event()
        self.__flush_task = Thread(target=self.__interval_flush, daemon=True)
        self.__flush_task.start()

    def __get_idx_name_func(self, param):
        if isinstance(param, str):
            return INDEX_NAME_FUNC_DICT[IndexNameFreq[param]]
        return INDEX_NAME_FUNC_DICT[param]

    def __get_auth_details(self, param):
        if isinstance(param, str):
            return AuthType[param]
        return param

    def __interval_flush(self):
        while not self.__stop_event.is_set():
            self.__stop_event.wait(self._flush_frequency)
            if self._should_flush(full=False):
                self.flush()

    # ---

    def __backup_restore(self, backup_filepath):
        try:
            with open(backup_filepath, 'r') as backup:
                self.buffer.extend(loads(log_record) for log_record in backup)
        except FileNotFoundError:
            pass
        finally:
            return open(backup_filepath, 'w')

    def __backup_store(self):
        self.flush_to_backup()
        self._backup_file.close()

    # ---

    def _get_actions(self, logs_buffer):
        for es_record in logs_buffer:
            yield {
                '_index': self._idx_name_func(self.es_idx_name),
                '_source': es_record
            }

    def __get_es_client(self):
        if self._client is not None:
            return self._client

        conn_params = {
            'hosts': self.hosts,
            'use_ssl': self.use_ssl,
            'verify_certs': self.verify_certs,
            'timeout': self._connection_timeout,
            'connection_class': self._connection_class,
            'serializer': ESSerializer()
        }

        if self.auth_type == AuthType.NO_AUTH:
            self._client = Elasticsearch(**conn_params)
            return self._client

        if self.auth_type == AuthType.BASIC_AUTH:
            self._client = Elasticsearch(**conn_params,
                                         http_auth=self.auth_details)
            return self._client

        raise ValueError("Authentication method not supported")

    def _pop_buffer(self):
        with self.lock:
            logs_buffer = self.buffer.copy()
            self.buffer.clear()
        return logs_buffer

    def flush(self):
        if self._is_flush_stop():
            return

        try:
            logs_buffer = self._pop_buffer()

            eshelpers.bulk(client=self.__get_es_client(),
                           actions=self._get_actions(logs_buffer),
                           stats_only=True)
        except Exception as exc:
            self.buffer.extend(logs_buffer)
            if self._raise_on_exceptions:
                self.flush_to_backup()
                raise exc

    def flush_to_backup(self):
        for es_record in self._pop_buffer():
            self._backup_file.write(f'{dumps(es_record)}\n')

    # ---

    def emit(self, log_record):
        self.buffer.append(self._log_record_to_es_fields(log_record))

        if self._is_flush_stop():
            self.flush_to_backup()
        else:
            if self._should_flush(full=True):
                self.flush()

    def close(self):
        self.__stop_event.set()
        self.__flush_task.join()
        self.__backup_store()

    # ---

    def _is_flush_stop(self):
        return self.__stop_event.is_set() or not self.__flush_task.is_alive()

    def _should_flush(self, *, full):
        if full:
            return len(self.buffer) >= self.capacity
        else:
            return bool(self.buffer)

    def _log_record_to_es_fields(self, log_record):
        es_record = self.es_add_fields.copy()

        es_record['@timestamp'] = _get_es_datetime_str(log_record.created)
        es_record['log.name'] = log_record.name
        es_record['log.level'] = log_record.levelname

        if isinstance(log_record.msg, dict):
            es_record.update(log_record.msg)
        else:
            es_record['message'] = log_record.msg

        if log_record.exc_info is not None:
            _, exc_value, traceback_obj = log_record.exc_info
            es_record['error.type'] = str(exc_value.__class__)
            es_record['error.message'] = str(exc_value)
            es_record['error.traceback'] = ''.join(fmtex(*log_record.exc_info))

        return es_record
