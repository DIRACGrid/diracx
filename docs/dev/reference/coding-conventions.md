# Coding Conventions

TODO: update

DO NOT FORGET TESTS

Most style are enforced with the pre-commit hooks which automatically reformat the code and sort the imports.
Code should follow PEP-8, particularly for [naming conventions](https://peps.python.org/pep-0008/#prescriptive-naming-conventions) as these aren't modified by the pre-commit hooks.

## Module Specifications

Depending on which module you are using, there could be rules to follow that are listed below.

<table>
<thead>
<tr>
    <th>Module</th>
    <th>Do</th>
    <th>Do not</th>
<tr>
</thead>

<tr>
<td>

`pytest`</td>

<td>

```python
import pytest


@pytest.fixture
def my_ficture():
    ...
```

</td>

<td>

```python
from pytest import fixture


@fixture
def my_ficture():
    ...
```

</td>
</tr>

<tr>
<td>

`datetime`

</td>

<td>

```python
from datetime import datetime, timedelta, timezone

delay = datetime.now(tz=timezone.utc) + timedelta(hours=1)
```

</td>

<td>

```python
import datetime

delay = datetime.datetime.now() + datetime.timedelta(hours=1)
```

</td>
</tr>

<tr>
<td>

`SQL Alchemy`

</td>

<td>

```python
class Owners(Base):
    __tablename__ = "Owners"
    owner_id: Mapped[int] = mapped_column(
        "OwnerID", Integer, primary_key=True, autoincrement=True
    )
    creation_time: Mapped[datetime_now] = mapped_column("CreationTime")
    name: Mapped[str255] = mapped_column("Name")
```

</td>

<td>

```python
class Owners(Base):
    __tablename__ = "Owners"
    OwnerID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    CreationTime: Mapped[datetime_now]
    Name: Mapped[str255]
```

</td>
</tr>

</table>

## Structure

The following structures principles may also be followed:

- `__init__.py` should not contain code, but `__all__` as a list
- At a package level (routers for example) we have as few files as possible
- The package level `__init__.py` should contain an empty `__all__` and the modules at that level should expose their respective public API
- If the files have much in common (think of jobs, which have the sandbox, the joblogging, etc), we put them in a subpackage (e.g routers.job). The code goes in a specific file (job.py, joblogging.py)
- At subpackage level, `__init__.py` should expose the public API through the `__all__` list by importing from the modules at that level. These submodules should not contain an `__all__`

See [this issue](https://github.com/DIRACGrid/diracx/issues/268).

## Architecture

To make sure that each part of DiracX is doing only what it is supposed to do, you also need to follow these instructions:

- `diracx-routers` should deal with user interactions through HTTPs. It is expected to deal with permissions and should call `diracx-logic`. Results returned should be translated into HTTP responses.
- `diracx-logic` should embed Dirac specificities. It should encapsulate the logic of the services and should call `diracx-db` to interact with databases.
- `diracx-db` should contain atomic methods (complex logic is expected to be located in `diracx-db`).
