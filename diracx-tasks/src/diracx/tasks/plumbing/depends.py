"""Canonical dependency injection type definitions.

``diracx.routers.dependencies`` re-exports from this module so that both
routers and the task worker resolve the same names.

DB classes, OS DB classes, and ``ServiceSettingsBase`` subclasses are
**auto-detected** by ``auto_inject_depends``:

- In tasks: called by ``wrap_task``
- In routers: called by ``DiracxRouter.add_api_route``
- In sub-dependency functions: applied via the ``@auto_inject`` decorator
"""

from __future__ import annotations

__all__ = (
    "AvailableSecurityProperties",
    "CallbackSpawner",
    "Config",
    "NoTransaction",
    "_CallbackSpawner",
    "_callback_spawner_placeholder",
    "auto_inject",
    "auto_inject_depends",
)

import dataclasses
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Annotated, TypeVar, get_args, get_origin

from fastapi import Depends
from fastapi.params import Depends as DependsClass

from diracx.core.config import Config as _Config
from diracx.core.config import ConfigSource
from diracx.core.properties import SecurityProperty
from diracx.core.settings import ServiceSettingsBase
from diracx.db.os.utils import BaseOSDB
from diracx.db.sql.utils import BaseSQLDB

from ._redis_types import CallbackRegistry

if TYPE_CHECKING:
    from .base_task import BaseTask as _BaseTask

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class NoTransaction:
    """Annotated marker: inject DB without auto-wrapping in a transaction.

    Usage::

        async def execute(
            self,
            db: Annotated[SandboxMetadataDB, NoTransaction()],
        ) -> int:
            async with db:  # caller manages transactions
                ...
    """


def auto_inject_depends(params: list[Parameter]) -> list[Parameter]:
    """Auto-wrap DB and Settings parameters with ``Depends``.

    Detects:
    - ``BaseSQLDB`` subclasses -> ``Depends(cls.transaction, scope="function")``
      (or ``cls.no_transaction`` when ``NoTransaction()`` marker is present)
    - ``BaseOSDB`` subclasses -> ``Depends(cls.session, scope="function")``
    - ``ServiceSettingsBase`` subclasses -> ``Depends(cls.create)``
    Already-wrapped ``Annotated[..., Depends(...)]`` types pass through
    unchanged.  Combining ``NoTransaction()`` with an explicit ``Depends``
    raises ``TypeError`` — use one or the other, not both.
    """
    result = []
    for p in params:
        annotation = p.annotation
        if annotation is Parameter.empty:
            result.append(p)
            continue

        base_type = annotation
        no_tx = False
        has_depends = False

        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            base_type = args[0]
            no_tx = any(isinstance(a, NoTransaction) for a in args[1:])
            has_depends = any(isinstance(a, DependsClass) for a in args[1:])

        if not isinstance(base_type, type):
            result.append(p)
            continue

        if issubclass(base_type, BaseSQLDB):
            if has_depends and no_tx:
                raise TypeError(
                    f"Parameter {p.name!r}: NoTransaction() cannot be combined "
                    f"with an explicit Depends() annotation"
                )
            if has_depends:
                result.append(p)
            else:
                dep = base_type.no_transaction if no_tx else base_type.transaction
                result.append(
                    p.replace(
                        annotation=Annotated[base_type, Depends(dep, scope="function")]
                    )
                )
        elif issubclass(base_type, BaseOSDB):
            if no_tx:
                raise TypeError(
                    f"Parameter {p.name!r}: NoTransaction() is not supported "
                    f"for BaseOSDB subclasses (they have no transaction semantics)"
                )
            if has_depends:
                result.append(p)
            else:
                result.append(
                    p.replace(
                        annotation=Annotated[
                            base_type,
                            Depends(base_type.session, scope="function"),
                        ]
                    )
                )
        elif issubclass(base_type, ServiceSettingsBase):
            if has_depends:
                result.append(p)
            else:
                result.append(
                    p.replace(
                        annotation=Annotated[base_type, Depends(base_type.create)]
                    )
                )
        else:
            result.append(p)

    return result


def auto_inject(func):
    """Adjust annotations to auto-inject ``Depends`` for DB and Settings parameters.

    Apply to functions used as FastAPI sub-dependencies that take
    DB or Settings classes as parameters.
    """
    sig = signature(func, eval_str=True)
    for p in auto_inject_depends(list(sig.parameters.values())):
        if p.annotation is not Parameter.empty:
            func.__annotations__[p.name] = p.annotation
    return func


# --- Types that cannot be auto-detected (no common base class pattern) ---

Config = Annotated[_Config, Depends(ConfigSource.create)]
AvailableSecurityProperties = Annotated[
    set[SecurityProperty], Depends(SecurityProperty.available_properties)
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
