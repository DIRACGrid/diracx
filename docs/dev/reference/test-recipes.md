# Test recipes

## UUIDv7 with frozen time

`freezegun` freezes the Python clock used by `datetime.now(...)`. A bare
`uuid_utils.uuid7()` may use a different clock path, so use
`diracx.testing.time.frozen_uuid7()` when a test needs the UUIDv7 timestamp to
follow frozen Python time. The helper reads the clock at each call, including
after `tick()`:

```python
from datetime import timedelta

from freezegun import freeze_time

from diracx.testing.time import frozen_uuid7

with freeze_time("2024-01-15 12:30:45.123") as frozen_time:
    first = frozen_uuid7()

    frozen_time.tick(timedelta(seconds=1))
    second = frozen_uuid7()
    assert second.timestamp == first.timestamp + 1000
```

If the code under test has already bound `uuid7` with
`from uuid_utils import uuid7`, patch that module's lookup site. For example,
`diracx.logic.__main__.new_key` looks up `uuid7` in `diracx.logic.__main__`:

```python
from diracx.logic import __main__ as key_management
from diracx.testing.time import frozen_uuid7

monkeypatch.setattr(key_management, "uuid7", frozen_uuid7)
```
