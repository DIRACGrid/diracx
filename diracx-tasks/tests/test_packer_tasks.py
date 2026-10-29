"""Tests for the transformation packer tasks."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from diracx.core.models.transformation import TransformationStatus
from diracx.logic.transformations import PackingSummary
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import load_task_registry
from diracx.tasks.plumbing.lock_registry import TRANSFORMATION
from diracx.tasks.plumbing.locks import ExclusiveRWLock, MutexLock, SharedRWLock
from diracx.tasks.transformations import PackerSweepTask, PackTransformationTask
from diracx.tasks.transformations import packer as packer_module


def test_tasks_are_registered():
    registry = load_task_registry()
    assert registry["transformation:PackerSweepTask"] is PackerSweepTask
    assert registry["transformation:PackTransformationTask"] is PackTransformationTask


def test_task_metadata():
    assert PackerSweepTask.priority == Priority.BACKGROUND
    assert PackerSweepTask.size == Size.SMALL
    assert PackerSweepTask._enabled is False
    assert PackTransformationTask.retry_policy.max_retries == 3
    assert PackTransformationTask.dlq_eligible is False


def test_serialize_round_trip():
    task = PackTransformationTask(transformation_id=str(uuid4()), flush=True)
    assert PackTransformationTask(*task.serialize()) == task


def test_pack_task_takes_a_packer_mutex_and_a_shared_status_lock():
    first, second = str(uuid4()), str(uuid4())
    mutex, status = PackTransformationTask(transformation_id=first).execution_locks
    assert isinstance(mutex, MutexLock)
    assert mutex.redis_key == f"lock:mutex:transformation:{first}:packer"
    assert isinstance(status, SharedRWLock)
    assert status.redis_key == f"lock:rw:transformation:{first}:status"
    # A status change takes the exclusive side of the same lock
    assert ExclusiveRWLock(TRANSFORMATION, first, "status").redis_key == (
        status.redis_key
    )
    other_mutex, other_status = PackTransformationTask(
        transformation_id=second
    ).execution_locks
    assert other_mutex.redis_key != mutex.redis_key
    assert other_status.redis_key != status.redis_key


def test_settings_must_be_positive():
    with pytest.raises(ValidationError):
        packer_module.PackerSettings(interval_seconds=0)
    with pytest.raises(ValidationError):
        packer_module.PackerSettings(max_inputs_per_invocation=0)


async def test_sweep_schedules_one_task_per_active_transformation(monkeypatch):
    active = [uuid4(), uuid4()]
    scheduled = []

    async def fake_schedule(self, **kwargs):
        scheduled.append(self)
        return "task-id"

    monkeypatch.setattr(PackTransformationTask, "schedule", fake_schedule)
    transformation_db = AsyncMock()
    transformation_db.list_transformation_ids.return_value = active

    assert await PackerSweepTask().execute(transformation_db) == 2

    transformation_db.list_transformation_ids.assert_awaited_once_with(
        TransformationStatus.ACTIVE
    )
    assert scheduled == [
        PackTransformationTask(transformation_id=str(transformation_id))
        for transformation_id in active
    ]


async def test_pack_task_delegates_to_logic(monkeypatch):
    pack_transformation = AsyncMock(
        return_value=PackingSummary(parcels_created=1, inputs_assigned=2)
    )
    monkeypatch.setattr(packer_module, "pack_transformation", pack_transformation)
    transformation_db = AsyncMock()
    transformation_id = uuid4()

    result = await PackTransformationTask(
        transformation_id=str(transformation_id), flush=True
    ).execute(transformation_db)

    assert result["parcels_created"] == 1
    assert result["inputs_assigned"] == 2
    pack_transformation.assert_awaited_once_with(
        transformation_db,
        UUID(str(transformation_id)),
        flush=True,
        max_inputs=packer_module._settings.max_inputs_per_invocation,
    )
