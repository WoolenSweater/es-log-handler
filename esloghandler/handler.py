import logging
from json import loads, dumps
from threading import Thread, Lock, Event
from traceback import format_exception as fmtex
from elasticsearch import helpers as eshelpers
from elasticsearch import Elasticsearch, Urllib3HttpConnection
from .utils import (AuthType, IndexNameFreq, ESSerializer, InvalidESIndexName,
                    INDEX_NAME_FUNC_DICT, _get_es_datetime_str)


class ESHandler(logging.Handler):
    def __init__(self,
                 *,
                 hosts=[{'host': 'localhost', 'port': 9200}],
                 auth_type=AuthType.NO_AUTH,
                 auth_details=None,
                 use_ssl=False,
                 verify_ssl=True,
                 buffer_size=1000,
                 connection=Urllib3HttpConnection,
                 flush_frequency_in_sec=1,
                 es_index_name=None,
                 es_index_name_frequency=IndexNameFreq.DAILY,
                 es_additional_fields={},
                 raise_on_exceptions=False,
                 backup_filepath='backup.log'):
        if not isinstance(es_index_name, str):
            raise InvalidESIndexName
        logging.Handler.__init__(self)

        self.hosts = hosts
        self.auth_details = auth_details
        self.auth_type = self.__get_auth_details(auth_type)

        self.use_ssl = use_ssl
        self.verify_certs = verify_ssl

        self.es_idx_name = es_index_name
        self.es_add_fields = es_additional_fields

        self._client = None
        self._buffer = []
        self._buffer_lock = Lock()
        self._buffer_size = buffer_size
        self._connection_class = connection
        self._flush_frequency = flush_frequency_in_sec
        self._idx_name_func = self.__get_idx_name_func(es_index_name_frequency)
        self._raise_on_exceptions = raise_on_exceptions

        self.__stop_event = Event()
        self.__flush_task = Thread(target=self.__interval_flush, daemon=True)
        self.__flush_task.start()

        self._backup_file = None
        self._backup_filepath = backup_filepath
        self.__backup_task = Thread(target=self.__backup_restore, daemon=True)
        self.__backup_task.start()

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
            self.flush()

    def __backup_restore(self):
        try:
            with open(self._backup_filepath, 'r') as backup:
                self._buffer.extend(loads(log_record) for log_record in backup)
        except FileNotFoundError:
            pass
        finally:
            self._backup_file = open(self._backup_filepath, 'w')

    def __get_es_client(self):
        if self._client is not None:
            return self._client

        conn_params = {
            'hosts': self.hosts,
            'use_ssl': self.use_ssl,
            'verify_certs': self.verify_certs,
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

    def flush(self):
        if self._buffer:
            try:
                with self._buffer_lock:
                    logs_buffer = self._buffer.copy()
                    self._buffer.clear()

                eshelpers.bulk(client=self.__get_es_client(),
                               actions=self._get_actions(logs_buffer),
                               stats_only=True)
            except Exception as exc:
                for es_record in logs_buffer:
                    self._backup_file.write(f'{dumps(es_record)}\n')
                if self._raise_on_exceptions:
                    raise exc

    def _get_actions(self, logs_buffer):
        for es_record in logs_buffer:
            yield {
                '_index': self._idx_name_func(self.es_idx_name),
                '_source': es_record
            }

    def close(self):
        self.__backup_task.join()
        self.__stop_event.set()
        self.__flush_task.join()
        self._backup_file.close()

    def emit(self, log_record):
        es_record = self._log_record_to_es_fields(log_record)

        if self.__stop_event.is_set() or not self.__flush_task.is_alive():
            self._backup_file.write(f'{dumps(es_record)}\n')
        else:
            with self._buffer_lock:
                self._buffer.append(es_record)

            if len(self._buffer) >= self._buffer_size:
                self.flush()

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
