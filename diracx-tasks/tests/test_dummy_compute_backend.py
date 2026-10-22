"""Tests for the dummy compute backend tasks."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select
from uuid_utils import UUID, uuid7

from diracx.core.models.transformation import (
    InputStatus,
    ParcelStatus,
    TransformationKind,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    Inputs,
    Ownership,
    ParcelInputs,
    Parcels,
    Transformations,
)
from diracx.tasks.plumbing.locks import MutexLock
from diracx.tasks.transformation import (
    DummyComputeBackendMonitorTask,
    DummyComputeBackendTask,
)
from diracx.tasks.transformation import (
    dummy_compute_backend as dummy_compute_backend_module,
)

FEATURE_ENABLED_ENV = "DIRACX_TASKS_DUMMY_COMPUTE_BACKEND_ENABLED"
FEATURE_INTERVAL_ENV = "DIRACX_TASKS_DUMMY_COMPUTE_BACKEND_INTERVAL_SECONDS"
SCHEDULER_STATE_SCRIPT = """
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

from diracx.tasks.plumbing.factory import load_task_registry
from diracx.tasks.plumbing.scheduler import TaskScheduler

task_name = "transformation:DummyComputeBackendMonitorTask"
registry = load_task_registry()
task_cls = registry[task_name]
scheduler = TaskScheduler(
    broker=MagicMock(),
    redis_url="redis://unused",
    task_registry=registry,
)
before = datetime.now(tz=UTC)
scheduler._compute_initial_schedules()
next_run = scheduler._next_runs.get((task_name, ""))
print(
    json.dumps(
        {
            "enabled": task_cls._enabled,
            "tracked": next_run is not None,
            "delay_seconds": (
                (next_run - before).total_seconds() if next_run is not None else None
            ),
        }
    )
)
"""


def get_scheduler_state(feature_env: dict[str, str]) -> dict:
    env = os.environ.copy()
    env.pop(FEATURE_ENABLED_ENV, None)
    env.pop(FEATURE_INTERVAL_ENV, None)
    env.update(feature_env)
    result = subprocess.run(
        [sys.executable, "-c", SCHEDULER_STATE_SCRIPT],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )
    return json.loads(result.stdout)


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
            await db.post_create(conn)
        yield db


@pytest.fixture
async def transformation_id(transformation_db: TransformationDB) -> UUID:
    ownership_id, transformation_id = uuid7(), uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Ownership).values(ownership_id=ownership_id, vo="lhcb", sub="me")
        )
        await db.conn.execute(
            insert(Transformations).values(
                transformation_id=transformation_id,
                ownership_id=ownership_id,
                type="MCSimulation",
                kind=TransformationKind.COMPUTE,
                packer="Standard",
            )
        )
    return transformation_id


async def add_parcel(
    db: TransformationDB,
    transformation_id: UUID,
    *,
    kind: TransformationKind = TransformationKind.COMPUTE,
    backend: str | None = None,
) -> UUID:
    """Insert an Unassigned parcel made of one Assigned input, as a packer would."""
    parcel_id, input_id = uuid7(), uuid7()
    await db.conn.execute(
        insert(Parcels).values(
            parcel_id=parcel_id,
            transformation_id=transformation_id,
            kind=kind,
            backend=backend,
        )
    )
    await db.conn.execute(
        insert(Inputs).values(
            transformation_id=transformation_id,
            input_id=input_id,
            input_hash=input_id.bytes,
            status=InputStatus.ASSIGNED,
            last_parcel_id=parcel_id,
        )
    )
    await db.conn.execute(
        insert(ParcelInputs).values(
            transformation_id=transformation_id,
            parcel_id=parcel_id,
            input_id=input_id,
        )
    )
    return parcel_id


async def parcel_state(db: TransformationDB, parcel_id: UUID):
    return (
        await db.conn.execute(
            select(Parcels.status, Parcels.backend).where(
                Parcels.parcel_id == parcel_id
            )
        )
    ).one()


async def input_status(db: TransformationDB, parcel_id: UUID) -> InputStatus:
    return await db.conn.scalar(
        select(Inputs.status).where(Inputs.last_parcel_id == parcel_id)
    )


@pytest.fixture
def scheduled(monkeypatch) -> list[tuple[str, datetime | None]]:
    """Capture the completion tasks the monitor schedules."""
    calls: list[tuple[str, datetime | None]] = []

    async def fake_schedule(self, *, at_time=None, labels=None):
        calls.append((self.parcel_id, at_time))
        return "task-id"

    monkeypatch.setattr(DummyComputeBackendTask, "schedule", fake_schedule)
    return calls


def test_monitor_schedule_activation_is_environment_controlled():
    default_state = get_scheduler_state({})
    assert default_state == {
        "enabled": False,
        "tracked": False,
        "delay_seconds": None,
    }

    local_state = get_scheduler_state(
        {FEATURE_ENABLED_ENV: "true", FEATURE_INTERVAL_ENV: "10"}
    )
    assert local_state["enabled"] is True
    assert local_state["tracked"] is True
    assert 9 <= local_state["delay_seconds"] <= 11


@pytest.mark.parametrize(
    "kwargs",
    [{"interval_seconds": 0}, {"max_parcels_per_cycle": 0}, {"runtime_seconds": -1}],
)
def test_settings_are_validated(kwargs):
    with pytest.raises(ValidationError):
        dummy_compute_backend_module.DummyComputeBackendSettings(**kwargs)


def test_completion_takes_a_per_parcel_mutex():
    parcel_id = str(uuid7())
    locks = DummyComputeBackendTask(parcel_id=parcel_id).execution_locks

    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert locks[0].redis_key == f"lock:mutex:parcel:{parcel_id}"


async def test_monitor_does_nothing_without_unassigned_parcels(
    transformation_db: TransformationDB, scheduled
):
    assert await DummyComputeBackendMonitorTask().execute(transformation_db) == 0
    assert scheduled == []


async def test_parcel_goes_from_unassigned_to_done(
    transformation_db: TransformationDB, transformation_id: UUID, scheduled
):
    async with transformation_db as db:
        first = await add_parcel(db, transformation_id)
        second = await add_parcel(db, transformation_id)
        # Neither of these is the dummy's to take
        data = await add_parcel(db, transformation_id, kind=TransformationKind.DATA)
        pinned = await add_parcel(db, transformation_id, backend="htcondor")

    # The monitor claims and submits the compute parcels, oldest first...
    before = datetime.now(UTC)
    assert await DummyComputeBackendMonitorTask().execute(transformation_db) == 2
    assert [parcel_id for parcel_id, _ in scheduled] == [str(first), str(second)]
    # ...and schedules their completion after the simulated run time
    for _, at_time in scheduled:
        assert at_time >= before
    async with transformation_db as db:
        assert await parcel_state(db, first) == (ParcelStatus.ASSIGNED, "dummy")
        assert await parcel_state(db, data) == (ParcelStatus.UNASSIGNED, None)
        assert await parcel_state(db, pinned) == (ParcelStatus.UNASSIGNED, "htcondor")

    # Nothing is claimed twice
    assert await DummyComputeBackendMonitorTask().execute(transformation_db) == 0

    # The completion task finishes the parcel and processes its input
    async with transformation_db as db:
        assert await DummyComputeBackendTask(parcel_id=str(first)).execute(db) is True
    async with transformation_db as db:
        assert await parcel_state(db, first) == (ParcelStatus.DONE, "dummy")
        assert await input_status(db, first) == InputStatus.PROCESSED
        # The other parcel waits for its own completion task
        assert await parcel_state(db, second) == (ParcelStatus.ASSIGNED, "dummy")
        assert await input_status(db, second) == InputStatus.ASSIGNED

    # Running the completion again, as a retry would, changes nothing
    async with transformation_db as db:
        assert await DummyComputeBackendTask(parcel_id=str(first)).execute(db) is False
    async with transformation_db as db:
        assert await parcel_state(db, first) == (ParcelStatus.DONE, "dummy")


async def test_monitor_respects_the_batch_size(
    transformation_db: TransformationDB, transformation_id: UUID, scheduled, monkeypatch
):
    monkeypatch.setattr(
        dummy_compute_backend_module,
        "_settings",
        dummy_compute_backend_module.DummyComputeBackendSettings(
            max_parcels_per_cycle=1
        ),
    )
    async with transformation_db as db:
        first = await add_parcel(db, transformation_id)
        second = await add_parcel(db, transformation_id)

    assert await DummyComputeBackendMonitorTask().execute(transformation_db) == 1
    assert await DummyComputeBackendMonitorTask().execute(transformation_db) == 1
    assert [parcel_id for parcel_id, _ in scheduled] == [str(first), str(second)]
