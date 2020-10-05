from enum import Enum
from datetime import timedelta, datetime as dt
from elasticsearch.serializer import JSONSerializer


class AuthType(Enum):
    NO_AUTH = 0
    BASIC_AUTH = 1


class IndexNameFreq(Enum):
    NEVER = 0
    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    YEARLY = 4


class ESSerializer(JSONSerializer):
    def default(self, data):
        try:
            return super(ESSerializer, self).default(data)
        except TypeError:
            return str(data)


class InvalidESIndexName(Exception):
    def __init__(self):
        super().__init__('Index name must be a string')


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
    return dt.utcfromtimestamp(ts).isoformat(timespec='milliseconds')


INDEX_NAME_FUNC_DICT = {
    IndexNameFreq.DAILY: _get_daily_index_name,
    IndexNameFreq.WEEKLY: _get_weekly_index_name,
    IndexNameFreq.MONTHLY: _get_monthly_index_name,
    IndexNameFreq.YEARLY: _get_yearly_index_name,
    IndexNameFreq.NEVER: _get_never_index_name
}
