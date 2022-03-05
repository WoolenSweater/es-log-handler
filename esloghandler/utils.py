from enum import IntEnum
from json import loads, dumps
from datetime import timedelta, datetime as dt
from elasticsearch.serializer import JSONSerializer


class IndexNameFreq(IntEnum):
    NEVER = 0
    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    YEARLY = 4


class File:
    
    def __init__(self, file):
        self._file = open(file, mode='a+')

    def read(self):
        self._file.seek(0)
        for record in self._file:
            yield loads(record)

        self._file.seek(0)
        self._file.truncate()

    def write(self, buffer):
        for record in buffer:
            self._file.write(f'{dumps(record)}\n')
        self._file.flush()


class ESSerializer(JSONSerializer):
    def default(self, data):
        try:
            return super(ESSerializer, self).default(data)
        except TypeError:
            return str(data)


def _get_daily_index_name(es_index_name):
    return f'{es_index_name}-{dt.now().strftime("%Y.%m.%d")}'


def _get_weekly_index_name(es_index_name):
    cur_date = dt.now()
    start_of_the_week = cur_date - timedelta(days=cur_date.weekday())
    return f'{es_index_name}-{start_of_the_week.strftime("%Y.%m.%d")}'


def _get_monthly_index_name(es_index_name):
    return f'{es_index_name}-{dt.now().strftime("%Y.%m")}'


def _get_yearly_index_name(es_index_name):
    return f'{es_index_name}-{dt.now().strftime("%Y")}'


def _get_never_index_name(es_index_name):
    return es_index_name


def _get_es_datetime_str(ts):
    return f'{dt.utcfromtimestamp(ts).isoformat(timespec="milliseconds")}Z'


INDEX_NAME_FUNCS = {
    IndexNameFreq.DAILY: _get_daily_index_name,
    IndexNameFreq.WEEKLY: _get_weekly_index_name,
    IndexNameFreq.MONTHLY: _get_monthly_index_name,
    IndexNameFreq.YEARLY: _get_yearly_index_name,
    IndexNameFreq.NEVER: _get_never_index_name
}
