from __future__ import annotations

__all__ = (
    "Config",
    "AuthDB",
    "JobDB",
    "JobLoggingDB",
    "SandboxMetadataDB",
    "TaskQueueDB",
    "add_settings_annotation",
    "AvailableSecurityProperties",
)

from typing import Annotated, TypeVar

from fastapi import Depends

from diracx.core.config import Config as _Config
from diracx.core.config import ConfigSource
from diracx.core.properties import SecurityProperty
from diracx.db.sql import AuthDB as _AuthDB
from diracx.db.sql import JobDB as _JobDB
from diracx.db.sql import JobLoggingDB as _JobLoggingDB
from diracx.db.sql import SandboxMetadataDB as _SandboxMetadataDB
from diracx.db.sql import TaskQueueDB as _TaskQueueDB

T = TypeVar("T")


def add_settings_annotation(cls: T) -> T:
    """Add a `Depends` annotation to a class that has a `create` classmethod."""
    return Annotated[cls, Depends(cls.create)]  # type: ignore


# Databases
AuthDB = Annotated[_AuthDB, Depends(_AuthDB.transaction)]
JobDB = Annotated[_JobDB, Depends(_JobDB.transaction)]
JobLoggingDB = Annotated[_JobLoggingDB, Depends(_JobLoggingDB.transaction)]
SandboxMetadataDB = Annotated[
    _SandboxMetadataDB, Depends(_SandboxMetadataDB.transaction)
]
TaskQueueDB = Annotated[_TaskQueueDB, Depends(_TaskQueueDB.transaction)]

# Miscellaneous
Config = Annotated[_Config, Depends(ConfigSource.create)]
AvailableSecurityProperties = Annotated[
    set[SecurityProperty], Depends(SecurityProperty.available_properties)
]
