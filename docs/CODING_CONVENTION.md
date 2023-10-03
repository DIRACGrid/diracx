Opinionated decisions

TODO:

# pytest

DO

```python
import pytest

@pytest.fixture
def my_ficture():
    ...
```

DONT

```python
from pytest import fixture

@fixture
def my_ficture():
    ...
```

# datetime

DO

```python
from datetime import datetime, timedelta
delay = datetime.now() + timedelta(hours=1)
```

DONT

```python
import datetime
delay = datetime.datetime.now() + datetime.timedelta(hours=1)
```


ALWAYS DO

```python
from __future__ import annotations
```


# Structure

(https://github.com/DIRACGrid/diracx/issues/268)

* `__init__.py` should not contain code, but `__all__`
* at a package level (router for example) we have one file per system (configuration.py for example)
* If we need more files (think of jobs, which have the sandbox, the joblogging, etc), we put them in a sub module (e.g routers.job). The code goes in a specific file (job.py, joblogging.py) but we use the the __init__.py to expose the specific file
