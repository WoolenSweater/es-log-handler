# Elasticsearch Log Handler

This library provides an Elasticsearch logging appender compatible with the python standard logging library.

### Inspired by:

* [Original lib (cmanaha)](https://github.com/cmanaha/python-elasticsearch-logger)
* [Fork lib (innovmetric)](https://github.com/innovmetric/python-elasticsearch-ecs-logger)

### Several big differences:

* Requirement Python >= 3.6
* No Kerberos Support
* No AWS Support
* Backup of the log to a file on sending error
* Log format (example below)

### Installation

```bash
pip install git+https://github.com/WoolenSweater/es-log-handler
```

### Requirements

* [requests](https://github.com/psf/requests)
* [elasticsearch](https://github.com/elastic/elasticsearch-py)

### Log format

If message is string:

```json
{
    "@timestamp": "2020-10-02T07:23:08.595",
    "log.name": "info",
    "log.level": "INFO",
    "message": "Job is done"
}
```

If exception:


```json
{
    "@timestamp": "2020-10-02T09:36:32.209",
    "log.name": "errors",
    "log.level": "ERROR",
    "message": "Expected Error",
    "error.type": "<class 'ValueError'>",
    "error.message": "failure",
    "error.traceback": "Traceback (most recent call last):\n  File \"test.py\", line 27, in error_middleware\n    return await handler(req)\n  File \"test.py\", line 11, in exception_handler\n    raise ValueError('failure')\nValueError: failure\n"}
```

If message is dict, dict unpack in root of record:

```json
{
    "@timestamp": "2020-10-02T10:23:08.595",
    "log.name": "info",
    "log.level": "INFO",
    "job": "job_id",
    "done": true
}
```

### Using the handler

```python
import logging
from esloghandler import ESHandler, AuthType, IndexNameFreq

log = logging.getLogger("important_info")
log.setLevel(logging.DEBUG)
log.addHandler(ESHandler(hosts=[{'host': 'localhost', 'port': 9200}],
                         auth_type=AuthType.BASIC_AUTH,
                         auth_details=('my_login', 'my_pass'),
                         use_ssl=False,
                         verify_ssl=True,
                         buffer_size=1000,
                         flush_frequency_in_sec=1,
                         es_index_name='app-dev-test-service',
                         es_index_name_frequency=IndexNameFreq.WEEKLY,
                         es_additional_fields={'additional_filed': 'if_need'},
                         backup_filepath='./logs/backup.log',
                         raise_on_exceptions=False))
log.info('Hello World')
log.info({'job': 123, 'done': True})
log.exception('Alarm')
```