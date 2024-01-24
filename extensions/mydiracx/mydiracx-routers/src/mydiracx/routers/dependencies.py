from __future__ import annotations

__all__ = ("DummyDB",)

from typing import Annotated, TypeVar

from fastapi import Depends

from mydiracx.db.sql import DummyDB as _DummyDB

T = TypeVar("T")


def add_settings_annotation(cls: T) -> T:
    """Add a `Depends` annotation to a class that has a `create` classmethod."""
    return Annotated[cls, Depends(cls.create)]  # type: ignore


# Databases
DummyDB = Annotated[_DummyDB, Depends(_DummyDB.transaction)]
