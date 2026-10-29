"""Tests for feeder sweep and feed tasks."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, ValidationError
from uuid_utils import uuid7

from diracx.core.exceptions import ConfigurationError
from diracx.core.models.feeder import Input, SetFeederState
from diracx.tasks.plumbing.locks import MutexLock, SharedRWLock
from diracx.tasks.transformations import FeedTransformation, PeriodicSweepFeeders
from diracx.tasks.transformations import feeder as feeder_tasks_module


class _FakeFeederArgs(BaseModel):
    max_count: int = 3


def _fake_feeder(
    *,
    feeder_args: _FakeFeederArgs,
    feeder_state: dict[str, Any],
    scouting: bool,
) -> Generator[Input | SetFeederState, None, None]:
    del feeder_args, scouting
    yield Input(descriptor={"seed": feeder_state.get("next_seed", 0)})
    yield SetFeederState(state={"next_seed": 1})


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
        lambda name: (_fake_feeder, _FakeFeederArgs),
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

    with pytest.raises(ConfigurationError, match="No feeder named"):
        load_feeder("DoesNotExistFeeder")


@pytest.fixture
async def transformation_db():
    from sqlalchemy import event

    from diracx.db.sql import TransformationDB

    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        event.listen(
            db.engine.sync_engine,
            "connect",
            lambda dbapi_conn, _: dbapi_conn.cursor().execute("PRAGMA foreign_keys=ON"),
        )
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
            await db.post_create(conn)
        yield db


async def test_feed_transformation_runs_integer_feeder_end_to_end(
    transformation_db,
):
    from sqlalchemy import insert, select

    from diracx.core.models.transformation import (
        TransformationKind,
        TransformationStatus,
    )
    from diracx.db.sql.transformations.schema import (
        Inputs,
        Ownership,
        Transformations,
    )

    ownership_id, transformation_id = uuid7(), uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Ownership).values(
                ownership_id=ownership_id,
                vo="lhcb",
                sub="integer-feeder-owner",
                workgraph_id=None,
            )
        )
        await db.conn.execute(
            insert(Transformations).values(
                transformation_id=transformation_id,
                ownership_id=ownership_id,
                type="MCSimulation",
                kind=TransformationKind.COMPUTE,
                status=TransformationStatus.ACTIVE,
                feeder="IntegerFeeder",
                feeder_args={"max_count": 3},
                feeder_state={"next_seed": 0},
                feeder_enabled=True,
                packer="Standard",
            )
        )

    await FeedTransformation(transformation_id=str(transformation_id)).execute(
        transformation_db=transformation_db
    )

    async with transformation_db as db:
        seeds = (
            await db.conn.execute(
                select(Inputs.descriptor.label("descriptor")).where(
                    Inputs.transformation_id == transformation_id
                )
            )
        ).all()
        columns = await db.get_feeder_columns(transformation_id)

    assert {row.descriptor["seed"] for row in seeds} == {0, 1, 2}
    assert columns is not None
    assert columns["feeder_enabled"] is False
    assert columns["feeder_state"] == {"next_seed": 3}
