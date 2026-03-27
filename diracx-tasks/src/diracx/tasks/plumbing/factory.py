from __future__ import annotations

__all__ = [
    "find_missing_overrides",
    "task_wrapper",
    "wrap_task",
    "load_task_registry",
    "setup_dependency_overrides",
]

import asyncio
import inspect
import logging
import os
from collections.abc import AsyncIterator, Iterable
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
from importlib.metadata import entry_points
from inspect import Parameter, signature
from typing import Any, Callable, Protocol, TypeVar

from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import get_dependant

from diracx.core.extensions import select_from_extension

from ._redis_types import LockCoordinator
from .base_task import BaseTask
from .broker.models import TaskBinding
from .locks import BaseLimiter, BaseLock

T = TypeVar("T")


class _AsyncContextManager(Protocol):
    async def __aenter__(self) -> Any: ...
    async def __aexit__(self, *args: Any) -> Any: ...


_T_DB = TypeVar("_T_DB", bound=_AsyncContextManager)

logger = logging.getLogger(__name__)


async def _lock_watchdog(
    held_locks: list[tuple[BaseLock, LockCoordinator]],
    stop_event: asyncio.Event,
    interval: float = 10.0,
) -> None:
    """Periodically extend lock TTLs while a task is running."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            pass
        for lock, redis_conn in held_locks:
            try:
                await lock.extend(redis_conn)
            except Exception:
                logger.warning(
                    "Failed to extend lock %s", lock.redis_key, exc_info=True
                )


async def task_wrapper(  # noqa: D417
    cls: type[BaseTask],
    *args: Any,
    _redis: LockCoordinator | None = None,
    _interactive: bool = False,
    **kwargs: Any,
) -> Any:
    """Instantiate a task, acquire locks, and execute it.

    ``args`` are the task's constructor arguments (from serialization).
    ``kwargs`` are resolved dependency injection dependencies for ``execute()``.

    Parameters
    ----------
        _redis: Redis connection for lock acquisition. When None, locks
            are skipped with a warning.
        _interactive: When True, ``BaseLimiter`` subclasses (rate limiters,
            concurrency limiters) are skipped entirely — only hard locks
            (mutex, RW) are acquired.

    """
    task = cls(*args)
    held_locks: list[tuple[BaseLock, LockCoordinator]] = []
    watchdog_task: asyncio.Task[None] | None = None
    try:
        for lock in task.execution_locks:
            # In interactive mode, skip limiters entirely
            if _interactive and isinstance(lock, BaseLimiter):
                continue

            if _redis is None:
                logger.warning("No Redis connection — skipping lock %s", lock.redis_key)
                continue

            acquired = await lock.acquire(_redis)
            if acquired:
                held_locks.append((lock, _redis))
            else:
                from .exceptions import UnableToAcquireLockError

                raise UnableToAcquireLockError(
                    f"Could not acquire lock {lock.redis_key}"
                )

        # Start watchdog to extend lock TTLs during execution
        stop_event = asyncio.Event()
        if held_locks:
            watchdog_task = asyncio.create_task(_lock_watchdog(held_locks, stop_event))

        result = await task.execute(**kwargs)
        return result
    finally:
        if watchdog_task is not None:
            stop_event.set()
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
        for lock, conn in held_locks:
            await lock.release(conn)


def wrap_task(cls: type[BaseTask]) -> Callable[..., Any]:
    """Create a wrapped callable from a BaseTask class.

    The wrapper has a modified signature that includes:
    - Positional args for task construction
    - Keyword-only params from ``execute()`` (for dependency injection resolution)

    Also attaches ``_dependant`` for FastAPI dependency resolution.
    """
    execute_sig = signature(cls.execute, eval_str=True)
    execute_params = list(execute_sig.parameters.values())
    if execute_params and execute_params[0].name == "self":
        execute_params = execute_params[1:]
    # Filter out VAR_KEYWORD (**kwargs) and VAR_POSITIONAL (*args) as these
    # are handled by the wrapper's own positional args/kwargs
    execute_params = [
        p
        for p in execute_params
        if p.kind not in (Parameter.VAR_KEYWORD, Parameter.VAR_POSITIONAL)
    ]

    parameters = [
        Parameter("args", Parameter.POSITIONAL_ONLY, default=()),
        Parameter("kwargs", Parameter.POSITIONAL_ONLY, default={}),
        *(p.replace(kind=Parameter.KEYWORD_ONLY) for p in execute_params),
    ]

    wrapped = partial(task_wrapper, cls)
    wrapped.__name__ = cls.__name__  # type: ignore[attr-defined]
    wrapped.__qualname__ = cls.__qualname__  # type: ignore[attr-defined]
    wrapped.__signature__ = execute_sig.replace(  # type: ignore[attr-defined]
        parameters=parameters,
        return_annotation=execute_sig.return_annotation,
    )

    wrapped._dependant = get_dependant(path="/", call=wrapped)  # type: ignore[attr-defined]

    return wrapped


def load_task_registry(
    groups: list[str] | None = None,
) -> dict[str, type[BaseTask]]:
    """Load task classes from entry points.

    Entry points are in groups like ``diracx.tasks.transformation``,
    ``diracx.tasks.jobs``, etc.

    Returns a dict of ``"group:ClassName" -> TaskClass``.
    """
    registry: dict[str, type[BaseTask]] = {}

    if groups is None:
        # Discover all diracx.tasks.* groups
        all_eps = entry_points()
        groups = [
            g
            for g in (all_eps.groups if hasattr(all_eps, "groups") else [])
            if g.startswith("diracx.tasks.")
        ]

    for group in groups:
        key = group.rsplit(".", 1)[-1]  # e.g. "transformation"
        for ep in select_from_extension(group=group):
            task_cls: type[BaseTask] = ep.load()
            task_name = f"{key}:{task_cls.__name__}"
            if task_name not in registry:  # Extension priority: first wins
                registry[task_name] = task_cls
                logger.debug("Loaded task: %s", task_name)

    return registry


def create_task_bindings(
    broker: Any,
    task_registry: dict[str, type[BaseTask]],
) -> tuple[dict[type[BaseTask], TaskBinding], dict[str, Callable[..., Any]]]:
    """Create task bindings and wrapped function registry.

    Returns:
        Tuple of (task_bindings, wrapped_registry)

    """
    task_bindings: dict[type[BaseTask], TaskBinding] = {}
    wrapped_registry: dict[str, Callable[..., Any]] = {}

    for task_name, task_cls in task_registry.items():
        wrapped_func = wrap_task(task_cls)

        binding = TaskBinding(
            broker=broker,
            task_name=task_name,
        )

        task_bindings[task_cls] = binding
        wrapped_registry[task_name] = wrapped_func

    return task_bindings, wrapped_registry


def find_missing_overrides(
    dependant: Dependant,
    overrides: dict[Callable, Callable],
) -> list[str]:
    """Check a dependency tree for unresolved dependencies.

    Returns a list of human-readable messages describing missing
    environment variables for each unresolved dependency.
    """
    from diracx.db.os.utils import BaseOSDB
    from diracx.db.sql.utils import BaseSQLDB

    missing: list[str] = []
    for sub in dependant.dependencies:
        if sub.call not in overrides:
            bound = getattr(sub.call, "__self__", None)
            if inspect.isclass(bound):
                if issubclass(bound, BaseSQLDB):
                    missing.append(
                        f"DIRACX_DB_URL_{bound.__name__.upper()} (for {bound.__name__})"
                    )
                elif issubclass(bound, BaseOSDB):
                    missing.append(
                        f"DIRACX_OS_DB_{bound.__name__.upper()} (for {bound.__name__})"
                    )
        missing.extend(find_missing_overrides(sub, overrides))
    return missing


def _find_dependents(dependant: Dependant, cls: type[T]) -> set[type[T]]:
    """Walk a Dependant tree and find all bound classes that are subclasses of *cls*."""
    found: set[type[T]] = set()
    for sub in dependant.dependencies:
        bound_class = getattr(sub.call, "__self__", None)
        if inspect.isclass(bound_class) and issubclass(bound_class, cls):
            found.add(bound_class)
        found |= _find_dependents(sub, cls)
    return found


async def _db_context(db: _T_DB) -> AsyncIterator[_T_DB]:
    """Yield a DB instance within its connection/transaction context."""
    async with db:
        yield db


@asynccontextmanager
async def setup_dependency_overrides(
    task_dependants: Iterable[Dependant] = (),
) -> AsyncIterator[dict[Callable, Callable]]:
    """Build dependency_overrides for task execution from environment variables.

    Sets up:
    - SQL databases (from ``DIRACX_DB_URL_*`` env vars)
    - OS databases (from ``DIRACX_OS_DB_*`` env vars)
    - Config (from ``DIRACX_CONFIG_BACKEND_URL`` env var)
    - Settings (pydantic models reading from env vars, discovered by
      scanning *task_dependants* for ``ServiceSettingsBase`` subclasses)
    """
    from diracx.db.os.utils import BaseOSDB
    from diracx.db.sql.utils import BaseSQLDB

    overrides: dict[Callable, Callable] = {}
    async with AsyncExitStack() as stack:
        # --- SQL databases ---
        for db_name, db_url in BaseSQLDB.available_urls().items():
            sql_db_classes = BaseSQLDB.available_implementations(db_name)
            sql_db = sql_db_classes[0](db_url=db_url)
            await stack.enter_async_context(sql_db.engine_context())
            for sql_db_class in sql_db_classes:
                overrides[sql_db_class.transaction] = partial(_db_context, sql_db)

        # --- OS databases ---
        for db_name, conn_kwargs in BaseOSDB.available_urls().items():
            os_db_classes = BaseOSDB.available_implementations(db_name)
            os_db = os_db_classes[0](connection_kwargs=conn_kwargs)
            await stack.enter_async_context(os_db.client_context())
            for os_db_class in os_db_classes:
                overrides[os_db_class.session] = partial(_db_context, os_db)

        # --- Config ---
        config_url = os.environ.get("DIRACX_CONFIG_BACKEND_URL")
        if config_url:
            from diracx.core.config import ConfigSource

            config_source = ConfigSource.create_from_url(backend_url=config_url)
            overrides[ConfigSource.create] = config_source.read_config

        # --- Settings ---
        # Scan task dependency trees to find which ServiceSettingsBase
        # subclasses are needed, then instantiate (pydantic reads env vars).
        from diracx.core.settings import ServiceSettingsBase

        settings_classes: set[type[ServiceSettingsBase]] = set()
        for dep in task_dependants:
            settings_classes |= _find_dependents(dep, ServiceSettingsBase)
        for settings_cls in settings_classes:
            try:
                instance = settings_cls()
                await stack.enter_async_context(instance.lifetime_function())
                overrides[settings_cls.create] = partial(lambda x: x, instance)
            except Exception:
                logger.debug(
                    "Settings %s not available, skipping",
                    settings_cls.__name__,
                    exc_info=True,
                )

        yield overrides
