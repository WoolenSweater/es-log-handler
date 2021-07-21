from logging import Handler
from threading import Thread, Event, Lock
from traceback import format_exception as fmtex
from elasticsearch import helpers as eshelpers
from elasticsearch import Elasticsearch, Urllib3HttpConnection
from esloghandler.utils import (
    INDEX_NAME_FUNCS,
    File,
    AuthType,
    IndexNameFreq,
    ESSerializer,
    _get_es_datetime_str
)


class ESHandler(Handler):
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
                 backup_filepath='backup.log'):
        if not isinstance(es_index_name, str):
            raise TypeError('es_index_name must be a string')

        if es_client is not None and not isinstance(es_client, Elasticsearch):
            raise TypeError('es_client must be Elasticsearch instance or None')

        Handler.__init__(self)

        self.hosts = hosts
        self.auth_details = auth_details
        self.auth_type = self.__get_auth_details(auth_type)

        self.use_ssl = use_ssl
        self.verify_certs = verify_ssl

        self._es_idx_name = es_index_name
        self._es_idx_name_func = self.__get_name_func(es_index_name_frequency)
        self._es_add_fields = es_additional_fields

        self._client = es_client
        self._connection_class = connection
        self._connection_timeout = connection_timeout
        self._last_sending_error = True

        self._backup_file = File(backup_filepath)

        self.__buffer = []
        self.__capacity = buffer_size
        self.__flush_frequency = flush_frequency_in_sec

        self.__lock = Lock()
        self.__stop_event = Event()
        self.__flush_task = Thread(target=self.__interval_flush, daemon=True)
        self.__flush_task.start()

    def __get_name_func(self, param):
        if isinstance(param, str):
            return INDEX_NAME_FUNCS[IndexNameFreq[param]]
        return INDEX_NAME_FUNCS[param]

    def __get_auth_details(self, param):
        if isinstance(param, str):
            return AuthType[param]
        return param

    def __wait(self):
        self.__stop_event.wait(self.__flush_frequency)

    def __interval_flush(self):
        while not self.__stop_event.is_set():
            if self._should_flush(full=False):
                self.flush()
            self.__wait()

    # ---

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

        raise ValueError('Authentication method not supported')

    def __get_actions(self, buffer):
        for es_record in buffer:
            yield {
                '_index': self._es_idx_name_func(self._es_idx_name),
                '_source': es_record
            }

    # ---

    def _pop_buffer(self):
        with self.__lock:
            buffer = self.__buffer.copy()
            self.__buffer.clear()
        return buffer

    def _store_to_backup(self, buffer):
        self._backup_file.write(buffer)
        self._last_sending_error = True

    def _restore_from_backup(self):
        self.__buffer.extend(self._backup_file.read())
        self._last_sending_error = False

    # ---

    def flush(self):
        try:
            if self._last_sending_error and self._client.ping():
                self._restore_from_backup()

            buffer = self._pop_buffer()
            eshelpers.bulk(client=self.__get_es_client(),
                           actions=self.__get_actions(buffer),
                           stats_only=True)
        except Exception:
            self._store_to_backup(buffer)

    def emit(self, log_record):
        self.__buffer.append(self._log_record_to_es_fields(log_record))

        if self._should_flush(full=True):
            self.flush()

    def close(self):
        self.__stop_event.set()
        self.__flush_task.join()

    # ---

    def _should_flush(self, *, full):
        if full:
            return len(self.__buffer) >= self.__capacity
        else:
            return bool(self.__buffer)

    def _log_record_to_es_fields(self, log_record):
        es_record = self._es_add_fields.copy()

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
