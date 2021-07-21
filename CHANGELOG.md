# CHANGELOG

### [0.3.0] - 2021-07-21

- Second version of the backup mechanism. Now, every time the flush method is called, before reading the buffer and sending, a check occurs. Was there an error when submitting last time and is the service available now. If the check is successful, past records from the backup file are returned to the buffer.
- Removed `raise_on_exceptions` parameter.

### [0.2.0] - 2021-04-06

- Added `connection_timeout` parameter.
- Removed "backup" thread. Backup mechanism has been rework.
- Refactoring.

### [0.1.2] - 2020-10-28

- Manimally working project.
