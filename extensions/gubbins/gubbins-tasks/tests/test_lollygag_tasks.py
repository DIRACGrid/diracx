"""Tests for gubbins lollygag task definitions."""

from __future__ import annotations

from unittest.mock import AsyncMock

from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import LockedObjectType
from diracx.tasks.plumbing.locks import MutexLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff

from gubbins.tasks.lock_types import LOLLYGAG
from gubbins.tasks.lollygag import OwnerCleanupTask, OwnerReportTask, SyncOwnersTask

# ---------------------------------------------------------------------------
# Lock type registration
# ---------------------------------------------------------------------------


def test_lollygag_lock_type_registered():
    """The LOLLYGAG lock type should be usable in LockedObjectType."""
    obj = LockedObjectType(LOLLYGAG)
    assert obj == "lollygag"


# ---------------------------------------------------------------------------
# SyncOwnersTask
# ---------------------------------------------------------------------------


def test_sync_owners_task_serialize():
    task = SyncOwnersTask(owner_name="alice")
    assert task.serialize() == ("alice",)


def test_sync_owners_task_properties():
    assert SyncOwnersTask.priority == Priority.NORMAL
    assert SyncOwnersTask.size == Size.SMALL
    assert isinstance(SyncOwnersTask.retry_policy, ExponentialBackoff)
    assert SyncOwnersTask.retry_policy.max_retries == 3
    assert SyncOwnersTask.dlq_eligible is True


def test_sync_owners_task_locks():
    task = SyncOwnersTask(owner_name="alice")
    locks = task.execution_locks
    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert "lollygag" in locks[0].redis_key
    assert "alice" in locks[0].redis_key


def test_sync_owners_task_different_owners_different_locks():
    lock_a = SyncOwnersTask(owner_name="alice").execution_locks[0]
    lock_b = SyncOwnersTask(owner_name="bob").execution_locks[0]
    assert lock_a.redis_key != lock_b.redis_key


async def test_sync_owners_task_execute():
    task = SyncOwnersTask(owner_name="bob")
    mock_db = AsyncMock()
    mock_db.insert_owner = AsyncMock(return_value=42)

    result = await task.execute(lollygag_db=mock_db)

    assert result == 42
    mock_db.insert_owner.assert_called_once_with("bob")


# ---------------------------------------------------------------------------
# OwnerCleanupTask
# ---------------------------------------------------------------------------


def test_owner_cleanup_task_schedule():
    assert OwnerCleanupTask.default_schedule.seconds == 3600


def test_owner_cleanup_task_locks():
    task = OwnerCleanupTask()
    locks = task.execution_locks
    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)


async def test_owner_cleanup_task_execute():
    task = OwnerCleanupTask()
    mock_db = AsyncMock()
    mock_db.get_owner = AsyncMock(return_value=["alice", "bob"])

    result = await task.execute(lollygag_db=mock_db)

    assert result == ["alice", "bob"]
    mock_db.get_owner.assert_called_once()


# ---------------------------------------------------------------------------
# OwnerReportTask (VO-aware periodic)
# ---------------------------------------------------------------------------


def test_owner_report_task_schedule():
    assert OwnerReportTask.default_schedule.expression == "0 6 * * *"


def test_owner_report_task_serialize():
    task = OwnerReportTask(vo="lhcb")
    assert task.serialize() == ("lhcb",)


def test_owner_report_task_locks_include_vo():
    task = OwnerReportTask(vo="lhcb")
    locks = task.execution_locks
    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert "lhcb" in locks[0].redis_key


def test_owner_report_task_different_vos_different_locks():
    lock_a = OwnerReportTask(vo="lhcb").execution_locks[0]
    lock_b = OwnerReportTask(vo="atlas").execution_locks[0]
    assert lock_a.redis_key != lock_b.redis_key


async def test_owner_report_task_execute():
    task = OwnerReportTask(vo="lhcb")
    mock_db = AsyncMock()
    mock_db.get_owner = AsyncMock(return_value=["alice"])

    result = await task.execute(lollygag_db=mock_db)

    assert result == ["alice"]
