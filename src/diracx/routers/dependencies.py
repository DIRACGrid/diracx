from __future__ import annotations

__all__ = ("Config", "AuthDB", "JobDB")

from typing import Annotated

from fastapi import Depends

from diracx.core.config import Config as _Config
from diracx.core.config import ConfigSource
from diracx.db import AuthDB as _AuthDB
from diracx.db import JobDB as _JobDB

Config = Annotated[_Config, Depends(ConfigSource.create)]

AuthDB = Annotated[_AuthDB, Depends(_AuthDB.transaction)]
JobDB = Annotated[_JobDB, Depends(_JobDB.transaction)]
