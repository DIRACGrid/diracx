"""Tests for feeder sweep and feed tasks."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError
from uuid_utils import uuid7

from diracx.core.exceptions import ResourceNotFoundError
from diracx.core.models.feeder import Input, SetFeederState
from diracx.tasks.plumbing.locks import MutexLock, SharedRWLock
from diracx.tasks.transformations import FeedTransformation, PeriodicSweepFeeders
from diracx.tasks.transformations import feeder as feeder_tasks_module


def test_feed_transformation_takes_feeder_mutex_and_shared_status_lock():
    transformation_id = str(uuid7())
    locks = FeedTransformation(transformation_id=transformation_id).execution_locks

    assert len(locks) == 2
    assert isinstance(locks[0], MutexLock)
    assert locks[0].redis_key == (
        f"lock:mutex:transformation:{transformation_id}:feeder"
    )
    assert isinstance(locks[1], SharedRWLock)
    assert locks[1].redis_key == f"lock:rw:transformation:{transformation_id}:status"


def test_sweep_interval_must_be_positive():
    with pytest.raises(ValidationError):
        feeder_tasks_module.FeederTaskSettings(interval_seconds=0)


async def test_sweep_schedules_one_feed_task_per_transformation(monkeypatch):
    tid_a, tid_b = uuid7(), uuid7()
    scheduled: list[str] = []

    async def fake_schedule(self, **kwargs):
        scheduled.append(self.transformation_id)
        return "task-id"

    monkeypatch.setattr(FeedTransformation, "schedule", fake_schedule)
    transformation_db = AsyncMock()
    transformation_db.list_feedable_transformation_ids.return_value = [tid_a, tid_b]

    result = await PeriodicSweepFeeders().execute(transformation_db=transformation_db)

    assert result == 2
    assert scheduled == [str(tid_a), str(tid_b)]


async def test_sweep_does_nothing_without_candidates(monkeypatch):
    scheduled: list[str] = []

    async def fake_schedule(self, **kwargs):
        scheduled.append(self.transformation_id)
        return "task-id"

    monkeypatch.setattr(FeedTransformation, "schedule", fake_schedule)
    transformation_db = AsyncMock()
    transformation_db.list_feedable_transformation_ids.return_value = []

    result = await PeriodicSweepFeeders().execute(transformation_db=transformation_db)

    assert result == 0
    assert scheduled == []


async def test_feed_runs_plugin_and_materialises(monkeypatch):
    transformation_id = uuid7()
    materialise = AsyncMock()
    monkeypatch.setattr(feeder_tasks_module, "materialise_feeder_yields", materialise)
    monkeypatch.setattr(
        feeder_tasks_module,
        "load_feeder",
        lambda name: (
            lambda *, feeder_args, feeder_state, scouting: iter(
                [
                    Input(descriptor={"seed": feeder_state.get("next_seed", 0)}),
                    SetFeederState(state={"next_seed": 1}),
                ]
            )
        ),
    )

    transformation_db = MagicMock()
    transformation_db.__aenter__ = AsyncMock(return_value=transformation_db)
    transformation_db.__aexit__ = AsyncMock(return_value=None)
    transformation_db.get_feeder_columns = AsyncMock(
        return_value={
            "feeder": "IntegerFeeder",
            "feeder_args": {"max_count": 3},
            "feeder_state": {"next_seed": 0},
            "feeder_enabled": True,
        }
    )

    await FeedTransformation(transformation_id=str(transformation_id)).execute(
        transformation_db=transformation_db
    )

    materialise.assert_awaited_once()
    args = materialise.await_args.args
    assert args[0] == transformation_id
    assert args[2] is transformation_db
    # Generator was passed through; consume to prove shape
    yields = list(args[1])
    assert yields[0].descriptor == {"seed": 0}
    assert yields[1].state == {"next_seed": 1}


async def test_feed_skips_when_disabled(monkeypatch):
    materialise = AsyncMock()
    load = MagicMock()
    monkeypatch.setattr(feeder_tasks_module, "materialise_feeder_yields", materialise)
    monkeypatch.setattr(feeder_tasks_module, "load_feeder", load)

    transformation_db = MagicMock()
    transformation_db.__aenter__ = AsyncMock(return_value=transformation_db)
    transformation_db.__aexit__ = AsyncMock(return_value=None)
    transformation_db.get_feeder_columns = AsyncMock(
        return_value={
            "feeder": "IntegerFeeder",
            "feeder_args": {},
            "feeder_state": {},
            "feeder_enabled": False,
        }
    )

    await FeedTransformation(transformation_id=str(uuid7())).execute(
        transformation_db=transformation_db
    )

    load.assert_not_called()
    materialise.assert_not_awaited()


async def test_feed_skips_when_transformation_missing(monkeypatch):
    materialise = AsyncMock()
    monkeypatch.setattr(feeder_tasks_module, "materialise_feeder_yields", materialise)

    transformation_db = MagicMock()
    transformation_db.__aenter__ = AsyncMock(return_value=transformation_db)
    transformation_db.__aexit__ = AsyncMock(return_value=None)
    transformation_db.get_feeder_columns = AsyncMock(return_value=None)

    await FeedTransformation(transformation_id=str(uuid7())).execute(
        transformation_db=transformation_db
    )

    materialise.assert_not_awaited()


async def test_load_feeder_unknown_name():
    from diracx.logic.transformations import load_feeder

    with pytest.raises(ResourceNotFoundError, match="feeder plugin"):
        load_feeder("DoesNotExistFeeder")
