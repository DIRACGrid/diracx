"""Tests for my_pilot task definitions."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import LockedObjectType
from diracx.tasks.plumbing.locks import MutexLock
from diracx.tasks.plumbing.retry_policies import NoRetry

from gubbins.logic.my_pilots import PilotSubmissionError
from gubbins.tasks.my_pilot_lock_types import MY_PILOT
from gubbins.tasks.my_pilots import (
    MyCheckPilotsTask,
    MyPilotReportTask,
    MyPilotTask,
    MySubmitPilotsTask,
)

# ---------------------------------------------------------------------------
# Lock type registration
# ---------------------------------------------------------------------------


def test_my_pilot_lock_type_registered():
    """The MY_PILOT lock type should be usable in LockedObjectType."""
    obj = LockedObjectType(MY_PILOT)
    assert obj == "my_pilot"


# ---------------------------------------------------------------------------
# MyPilotTask
# ---------------------------------------------------------------------------


def test_my_pilot_task_serialize():
    task = MyPilotTask(ce_name="reliable-ce.example.org")
    assert task.serialize() == ("reliable-ce.example.org",)


def test_my_pilot_task_properties():
    assert MyPilotTask.priority == Priority.NORMAL
    assert MyPilotTask.size == Size.SMALL
    assert isinstance(MyPilotTask.retry_policy, NoRetry)
    assert MyPilotTask.dlq_eligible is False


def test_my_pilot_task_locks():
    task = MyPilotTask(ce_name="reliable-ce.example.org")
    locks = task.execution_locks
    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert "my_pilot" in locks[0].redis_key
    assert "reliable-ce.example.org" in locks[0].redis_key


def test_my_pilot_task_different_ces_different_locks():
    lock_a = MyPilotTask(ce_name="ce-a").execution_locks[0]
    lock_b = MyPilotTask(ce_name="ce-b").execution_locks[0]
    assert lock_a.redis_key != lock_b.redis_key


async def test_my_pilot_task_execute_success():
    """With success_rate=1.0, submission always succeeds."""
    task = MyPilotTask(ce_name="reliable-ce.example.org")
    mock_db = AsyncMock()
    mock_db.get_ce_success_rate = AsyncMock(return_value=1.0)
    mock_db.submit_pilot = AsyncMock(return_value=42)

    result = await task.execute(my_pilot_db=mock_db)

    assert result == 42
    mock_db.submit_pilot.assert_called_once_with("reliable-ce.example.org")


async def test_my_pilot_task_execute_failure():
    """With success_rate=0.0, submission always fails."""
    task = MyPilotTask(ce_name="flaky-ce.example.org")
    mock_db = AsyncMock()
    mock_db.get_ce_success_rate = AsyncMock(return_value=0.0)

    with pytest.raises(PilotSubmissionError, match="failed"):
        await task.execute(my_pilot_db=mock_db)

    mock_db.submit_pilot.assert_not_called()


# ---------------------------------------------------------------------------
# MyPilotReportTask
# ---------------------------------------------------------------------------


def test_my_pilot_report_task_schedule():
    assert MyPilotReportTask.default_schedule.expression == "0 * * * *"


def test_my_pilot_report_task_locks():
    task = MyPilotReportTask()
    locks = task.execution_locks
    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)


async def test_my_pilot_report_task_execute():
    task = MyPilotReportTask()
    mock_db = AsyncMock()
    expected = {"submitted": 5, "running": 3}
    mock_db.get_pilot_summary = AsyncMock(return_value=expected)

    result = await task.execute(my_pilot_db=mock_db)

    assert result == expected
    mock_db.get_pilot_summary.assert_called_once()


# ---------------------------------------------------------------------------
# MyCheckPilotsTask (VO-aware periodic)
# ---------------------------------------------------------------------------


def test_my_check_pilots_task_schedule():
    assert MyCheckPilotsTask.default_schedule.seconds == 30


def test_my_check_pilots_task_serialize():
    task = MyCheckPilotsTask(vo="lhcb")
    assert task.serialize() == ("lhcb",)


def test_my_check_pilots_task_locks_include_vo():
    task = MyCheckPilotsTask(vo="lhcb")
    locks = task.execution_locks
    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert "lhcb" in locks[0].redis_key


def test_my_check_pilots_task_different_vos_different_locks():
    lock_a = MyCheckPilotsTask(vo="lhcb").execution_locks[0]
    lock_b = MyCheckPilotsTask(vo="atlas").execution_locks[0]
    assert lock_a.redis_key != lock_b.redis_key


# ---------------------------------------------------------------------------
# MySubmitPilotsTask (VO-aware periodic, spawns children)
# ---------------------------------------------------------------------------


def test_my_submit_pilots_task_schedule():
    assert MySubmitPilotsTask.default_schedule.seconds == 60


def test_my_submit_pilots_task_serialize():
    task = MySubmitPilotsTask(vo="lhcb")
    assert task.serialize() == ("lhcb",)


def test_my_submit_pilots_task_locks_include_vo():
    task = MySubmitPilotsTask(vo="lhcb")
    locks = task.execution_locks
    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert "lhcb" in locks[0].redis_key
