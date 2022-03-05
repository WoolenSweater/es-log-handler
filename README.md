# Elasticsearch Log Handler

![GitHub](https://img.shields.io/github/license/WoolenSweater/es-log-handler)
![Python](https://img.shields.io/badge/python-%3E%3D3.6-blue)

---

This library provides an Elasticsearch logging appender compatible with the python standard logging library.

### Inspired by:

* [Original lib (cmanaha)](https://github.com/cmanaha/python-elasticsearch-logger)
* [Fork lib (IMInterne)](https://github.com/IMInterne/python-elasticsearch-ecs-logger)

### Several big differences:

* Requirement Python >= 3.6
* **Since version 0.4.0 elasticsearch>=8 is required**
* No Kerberos and AWS inside (if need, create an Elasticsearch client and pass to the handler)
* Backup of the log to a file on sending error
* Log format (example below)

### Installation

```bash
pip install -i https://test.pypi.org/simple/ es-log-handler
```

### Requirements

* [elasticsearch](https://github.com/elastic/elasticsearch-py)

### Log format

If message is string:

```json
{
    "@timestamp": "2020-10-02T07:23:08.595Z",
    "log.name": "info",
    "log.level": "INFO",
    "message": "Job is done"
}
```

If exception:

```json
{
    "@timestamp": "2020-10-02T09:36:32.209Z",
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
    "@timestamp": "2020-10-02T10:23:08.595Z",
    "log.name": "info",
    "log.level": "INFO",
    "job": "job_id",
    "done": true
}
```

### Using the handler

```python
import logging
from esloghandler import ESHandler

log = logging.getLogger('important_info')
log.setLevel(logging.DEBUG)
log.addHandler(ESHandler('http://localhost:9200',
                         basic_auth=('my_login', 'my_pass'),
                         es_index_name='app-dev-test-service',
                         es_index_name_frequency='WEEKLY'))
log.info('Hello World')
log.info({'job': 123, 'done': True})
log.exception('Alarm')
```