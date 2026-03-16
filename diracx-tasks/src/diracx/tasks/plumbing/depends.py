"""Canonical dependency injection type definitions for tasks.

These are the same Annotated types used in ``diracx.routers.dependencies``
but defined here so that the task worker can resolve them without importing
the router machinery.

``diracx.routers.dependencies`` should re-export from this module.
"""

from __future__ import annotations

__all__ = (
    "Config",
    "AuthDB",
    "JobDB",
    "JobLoggingDB",
    "SandboxMetadataDB",
    "TaskQueueDB",
    "PilotAgentsDB",
    "JobParametersDB",
    "DBDepends",
    "add_settings_annotation",
    "AvailableSecurityProperties",
    "AuthSettings",
    "DevelopmentSettings",
    "SandboxStoreSettings",
    "CallbackSpawner",
    "_CallbackSpawner",
    "_callback_spawner_placeholder",
)

from functools import partial
from typing import TYPE_CHECKING, Annotated, TypeVar

from fastapi import Depends

from diracx.core.config import Config as _Config
from diracx.core.config import ConfigSource
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings as _AuthSettings
from diracx.core.settings import DevelopmentSettings as _DevelopmentSettings
from diracx.core.settings import SandboxStoreSettings as _SandboxStoreSettings
from diracx.db.os import JobParametersDB as _JobParametersDB
from diracx.db.sql import AuthDB as _AuthDB
from diracx.db.sql import JobDB as _JobDB
from diracx.db.sql import JobLoggingDB as _JobLoggingDB
from diracx.db.sql import PilotAgentsDB as _PilotAgentsDB
from diracx.db.sql import SandboxMetadataDB as _SandboxMetadataDB
from diracx.db.sql import TaskQueueDB as _TaskQueueDB

from ._redis_types import CallbackRegistry

if TYPE_CHECKING:
    from .base_task import BaseTask as _BaseTask

T = TypeVar("T")

# Use scope="function" to ensure DB commits happen before sending HTTP responses
# This prevents race conditions when DIRAC immediately queries data after DiracX writes it
DBDepends = partial(Depends, scope="function")


def add_settings_annotation(cls: T) -> T:
    """Add a ``Depends`` annotation to a class that has a ``create`` classmethod."""
    return Annotated[cls, Depends(cls.create)]  # type: ignore[return-value, attr-defined]


# Databases
AuthDB = Annotated[_AuthDB, DBDepends(_AuthDB.transaction)]
JobDB = Annotated[_JobDB, DBDepends(_JobDB.transaction)]
JobLoggingDB = Annotated[_JobLoggingDB, DBDepends(_JobLoggingDB.transaction)]
PilotAgentsDB = Annotated[_PilotAgentsDB, DBDepends(_PilotAgentsDB.transaction)]
SandboxMetadataDB = Annotated[
    _SandboxMetadataDB, DBDepends(_SandboxMetadataDB.transaction)
]
TaskQueueDB = Annotated[_TaskQueueDB, DBDepends(_TaskQueueDB.transaction)]

# Opensearch databases
JobParametersDB = Annotated[_JobParametersDB, DBDepends(_JobParametersDB.session)]


# Miscellaneous
Config = Annotated[_Config, Depends(ConfigSource.create)]
AvailableSecurityProperties = Annotated[
    set[SecurityProperty], Depends(SecurityProperty.available_properties)
]

AuthSettings = Annotated[_AuthSettings, Depends(_AuthSettings.create)]
DevelopmentSettings = Annotated[
    _DevelopmentSettings, Depends(_DevelopmentSettings.create)
]
SandboxStoreSettings = Annotated[
    _SandboxStoreSettings, Depends(_SandboxStoreSettings.create)
]


# Callback spawner
class _CallbackSpawner:
    """Spawn child tasks with a callback, with the Redis connection already bound."""

    def __init__(self, redis: CallbackRegistry):
        self._redis = redis

    async def __call__(
        self,
        children: list[_BaseTask],
        callback: _BaseTask,
        *,
        ttl_seconds: int = 86400,
    ) -> str:
        from .callbacks import spawn_with_callback

        return await spawn_with_callback(
            children, callback, redis=self._redis, ttl_seconds=ttl_seconds
        )


async def _callback_spawner_placeholder() -> _CallbackSpawner:
    """Raise because this must be overridden via broker.dependency_overrides."""
    raise RuntimeError(
        "CallbackSpawner can only be resolved inside a task worker. "
        "Register an override in broker.dependency_overrides."
    )


CallbackSpawner = Annotated[_CallbackSpawner, Depends(_callback_spawner_placeholder)]
