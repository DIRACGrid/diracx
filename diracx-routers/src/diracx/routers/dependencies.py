from __future__ import annotations

__all__ = (
    "Config",
    "AuthDB",
    "JobDB",
    "JobLoggingDB",
    "SandboxMetadataDB",
    "TaskQueueDB",
    "PilotAgentsDB",
    "add_settings_annotation",
    "AvailableSecurityProperties",
)

from typing import Annotated, TypeVar

from fastapi import Depends

from diracx.backend.dal.os import JobParametersDB as _JobParametersDB
from diracx.backend.dal.sql import AuthDB as _AuthDB
from diracx.backend.dal.sql import JobDB as _JobDB
from diracx.backend.dal.sql import JobLoggingDB as _JobLoggingDB
from diracx.backend.dal.sql import PilotAgentsDB as _PilotAgentsDB
from diracx.backend.dal.sql import SandboxMetadataDB as _SandboxMetadataDB
from diracx.backend.dal.sql import TaskQueueDB as _TaskQueueDB
from diracx.core.config import Config as _Config
from diracx.core.config import ConfigSource
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings as _AuthSettings
from diracx.core.settings import DevelopmentSettings as _DevelopmentSettings

T = TypeVar("T")


def add_settings_annotation(cls: T) -> T:
    """Add a `Depends` annotation to a class that has a `create` classmethod."""
    return Annotated[cls, Depends(cls.create)]  # type: ignore


# Databases
AuthDB = Annotated[_AuthDB, Depends(_AuthDB.transaction)]
JobDB = Annotated[_JobDB, Depends(_JobDB.transaction)]
JobLoggingDB = Annotated[_JobLoggingDB, Depends(_JobLoggingDB.transaction)]
PilotAgentsDB = Annotated[_PilotAgentsDB, Depends(_PilotAgentsDB.transaction)]
SandboxMetadataDB = Annotated[
    _SandboxMetadataDB, Depends(_SandboxMetadataDB.transaction)
]
TaskQueueDB = Annotated[_TaskQueueDB, Depends(_TaskQueueDB.transaction)]

# Opensearch databases
JobParametersDB = Annotated[_JobParametersDB, Depends(_JobParametersDB.session)]


# Miscellaneous
Config = Annotated[_Config, Depends(ConfigSource.create)]
AvailableSecurityProperties = Annotated[
    set[SecurityProperty], Depends(SecurityProperty.available_properties)
]

AuthSettings = Annotated[_AuthSettings, Depends(_AuthSettings.create)]
DevelopmentSettings = Annotated[
    _DevelopmentSettings, Depends(_DevelopmentSettings.create)
]
