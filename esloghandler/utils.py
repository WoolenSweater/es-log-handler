from enum import Enum
from datetime import timedelta, datetime as dt
from elasticsearch.serializer import JSONSerializer


class AuthType(Enum):
    NO_AUTH = 0
    BASIC_AUTH = 1


class IndexNameFreq(Enum):
    DAILY = 0
    WEEKLY = 1
    MONTHLY = 2
    YEARLY = 3
    NEVER = 4


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
    return dt.utcfromtimestamp(ts).isoformat(timespec='milliseconds')
