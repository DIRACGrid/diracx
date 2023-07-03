from __future__ import annotations

__all__ = (
    "Config",
    "AuthDB",
    "JobDB",
    "AvailableSecurityProperties",
)

from typing import Annotated, TypeVar

from fastapi import Depends

from diracx.core.config import Config as _Config
from diracx.core.config import ConfigSource
from diracx.core.properties import SecurityProperty
from diracx.db import AuthDB as _AuthDB
from diracx.db import JobDB as _JobDB

T = TypeVar("T")


# Databases
AuthDB = Annotated[_AuthDB, Depends(_AuthDB.transaction)]
JobDB = Annotated[_JobDB, Depends(_JobDB.transaction)]

# Miscellaneous
Config = Annotated[_Config, Depends(ConfigSource.create)]
AvailableSecurityProperties = Annotated[
    set[SecurityProperty], Depends(SecurityProperty.available_properties)
]
