"""Tests for the correct Worker handling of the exit stack."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import Annotated
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import Depends

from diracx.db.sql.dummy.db import DummyDB
from diracx.db.sql.dummy.schema import Owners
from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.broker.models import TaskMessage
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import (
    _db_context,
    _db_no_transaction,
    wrap_task,
)
from diracx.tasks.plumbing.retry_policies import NoRetry
from diracx.tasks.plumbing.worker.worker import Worker

# ---------------------------------------------------------------------------
# Shared state, reset before every test
# ---------------------------------------------------------------------------

# The type of exception thrown in, or ``None`` for a normal exit.
_exits: list[type[BaseException] | None] = []

# Events used to synchronise tests with tasks.
_events: dict[str, asyncio.Event] = {}


@pytest.fixture(autouse=True)
def _reset_tracking():
    _exits.clear()
    _events["started"] = asyncio.Event()
    _events["cleanup_started"] = asyncio.Event()
    yield
    _exits.clear()


# ---------------------------------------------------------------------------
# Dependencies, helpers and dummy DB declaration
# ---------------------------------------------------------------------------


async def _tracked_dep():
    """Record how the worker closes this dependency."""
    try:
        yield "resource"
    except BaseException as exc:
        _exits.append(type(exc))
        raise
    else:
        _exits.append(None)


async def _slow_cleanup_dep():
    """A dependency whose cleanup hangs until it is cancelled."""
    try:
        yield "resource"
    finally:
        _events["cleanup_started"].set()
        await asyncio.Event().wait()


async def _failing_cleanup_dep():
    """A dependency whose cleanup always raises a new exception."""
    try:
        yield "resource"
    finally:
        raise RuntimeError("cleanup failed")


def _build_worker(task_cls, broker, task_class_registry, wrapped_registry):
    """Return ``(worker, wrapped_task, task_message)`` for an inline task class."""
    name = f"test:{task_cls.__name__}"
    wrapped = wrap_task(task_cls)
    worker = Worker(
        broker=broker,
        task_registry={**wrapped_registry, name: wrapped},
        task_class_registry={**task_class_registry, name: task_cls},
    )
    message = TaskMessage(
        task_id=f"t-{task_cls.__name__}",
        task_name=name,
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
    )
    return worker, wrapped, message


def _mock_redis(*, lock_acquired: bool = True) -> AsyncMock:
    """Mock redis with AsyncMock()."""
    redis = AsyncMock()
    redis.__aenter__ = AsyncMock(return_value=redis)
    redis.__aexit__ = AsyncMock(return_value=False)
    redis.set = AsyncMock(return_value=True if lock_acquired else None)
    redis.zadd = AsyncMock()
    return redis


async def _run(worker, wrapped, message, redis=None):
    """Run a task."""
    with patch.object(worker, "_get_redis", return_value=redis or _mock_redis()):
        return await worker.run_task(wrapped, message)


def _error_type(result) -> str | None:
    """Get the error type."""
    return (result.error or {}).get("type")


async def _wait_for(event_name: str) -> None:
    await asyncio.wait_for(_events[event_name].wait(), timeout=5)


@pytest.fixture
async def dummy_db(tmp_path, broker):
    """A DummyDB on a file-backed SQLite DB, injected through the broker."""
    db = DummyDB(db_url=f"sqlite+aiosqlite:///{tmp_path / 'dummy.db'}")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        broker.dependency_overrides[DummyDB.transaction] = partial(_db_context, db)
        broker.dependency_overrides[DummyDB.no_transaction] = partial(
            _db_no_transaction, db
        )
        yield db


# ---------------------------------------------------------------------------
# Tasks used in the dests. Using dependencies or an SQL database (DummyDB)
# ---------------------------------------------------------------------------


class _FailingWithDep(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, res: Annotated[str, Depends(_tracked_dep)]) -> str:
        raise ValueError("error-value")


class _SucceedingWithDep(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, res: Annotated[str, Depends(_tracked_dep)]) -> str:
        return "ok"


class _BlockingWithDep(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, res: Annotated[str, Depends(_tracked_dep)]) -> str:
        _events["started"].set()
        await asyncio.Event().wait()  # until cancelled
        return "unreachable"


class _SucceedingWithFailingCleanup(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, res: Annotated[str, Depends(_failing_cleanup_dep)]) -> str:
        return "ok"


class _InsertOwnerThenFail(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, db: DummyDB) -> None:
        await db.insert_owner("owner-fail")
        raise ValueError("error-value")


class _InsertOwnerAndSucceed(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, db: DummyDB) -> str:
        await db.insert_owner("owner-ok")
        return "ok"


class _InsertOwnerThenBlock(BaseTask):
    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()

    async def execute(self, db: DummyDB) -> str:
        await db.insert_owner("owner-cancel")
        _events["started"].set()
        await asyncio.Event().wait()  # until cancelled
        return "unreachable"


# ---------------------------------------------------------------------------
# Actual tests
# ---------------------------------------------------------------------------


async def test_worker_throws_task_exception_into_dependencies(
    broker, task_class_registry, wrapped_registry
):
    """A failing task's exception must reach the cleanup of its dependencies."""
    worker, wrapped, message = _build_worker(
        _FailingWithDep, broker, task_class_registry, wrapped_registry
    )

    result = await _run(worker, wrapped, message)

    assert result.is_err
    assert _error_type(result) == "ValueError"
    # A failing task must report an actual error, not `None`
    assert _exits == [ValueError]


async def test_worker_closes_dependencies_normally_on_success(
    broker, task_class_registry, wrapped_registry
):
    """A successful task must close its dependencies without an exception."""
    worker, wrapped, message = _build_worker(
        _SucceedingWithDep, broker, task_class_registry, wrapped_registry
    )

    result = await _run(worker, wrapped, message)

    assert not result.is_err
    assert result.return_value == "ok"
    # A failing task must report `None`
    assert _exits == [None]


async def test_worker_cancelled_task_unwinds_dependencies(
    broker, task_class_registry, wrapped_registry
):
    """A cancelled task is reported as such and unwinds its dependencies."""
    worker, wrapped, message = _build_worker(
        _BlockingWithDep, broker, task_class_registry, wrapped_registry
    )

    with patch.object(worker, "_get_redis", return_value=_mock_redis()):
        run = asyncio.create_task(worker.run_task(wrapped, message))
        await _wait_for("started")
        run.cancel()
        # The first cancellation is recorded by _execute_task and turned
        # into an error result, it does not propagate out of run_task.
        result = await run

    assert result.is_err
    assert _error_type(result) == "CancelledError"
    # The returned error from a cancelled task should be `asyncio.CancelledError`
    assert _exits == [asyncio.CancelledError]


async def test_worker_reports_error_when_cleanup_fails_after_success(
    broker, task_class_registry, wrapped_registry
):
    """A task whose dependencies fail to close (e.g. a failed commit) is an error."""
    worker, wrapped, message = _build_worker(
        _SucceedingWithFailingCleanup, broker, task_class_registry, wrapped_registry
    )

    result = await _run(worker, wrapped, message)

    assert result.is_err
    # Failing to close dependencies (e.g. cleanup) must return an error
    assert _error_type(result) == "RuntimeError"


async def test_worker_rolls_back_failed_task_and_db_stays_usable(
    broker, task_class_registry, wrapped_registry, dummy_db
):
    """After a failed task the DB must be reusable and the writes made by the failing task must be rolled back."""
    failing = _build_worker(
        _InsertOwnerThenFail, broker, task_class_registry, wrapped_registry
    )
    succeeding = _build_worker(
        _InsertOwnerAndSucceed, broker, task_class_registry, wrapped_registry
    )

    first = await _run(*failing)
    second = await _run(*succeeding)

    assert first.is_err
    assert not second.is_err
    # Only the second _run is successful and commits to the DB
    async with dummy_db as dummy_db:
        owners = await dummy_db.summary(["Name"], [], Owners)
        assert owners[0]["Name"] == "owner-ok"


async def test_worker_rolls_back_db_when_task_is_cancelled(
    broker, task_class_registry, wrapped_registry, dummy_db
):
    """Writes made by a cancelled task must not be committed."""
    worker, wrapped, message = _build_worker(
        _InsertOwnerThenBlock, broker, task_class_registry, wrapped_registry
    )

    with patch.object(worker, "_get_redis", return_value=_mock_redis()):
        run = asyncio.create_task(worker.run_task(wrapped, message))
        await _wait_for("started")
        run.cancel()
        result = await run

    assert result.is_err
    assert _error_type(result) == "CancelledError"
    # A cancelled task must rolled back the insert
    async with dummy_db as dummy_db:
        owners = await dummy_db.summary(["OwnerID"], [], Owners)
        assert len(owners) == 0
