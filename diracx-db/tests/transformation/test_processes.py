from __future__ import annotations

import pytest
from sqlalchemy import func, insert, select
from uuid_utils import uuid7

from diracx.core.models.transformation import ParcelStatus, TransformationStatus
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    ComputeParcels,
    ComputeTransformations,
    Ownership,
    Parcels,
    Processes,
    TransformationLog,
    Transformations,
    Workgraphs,
)

OLD, NEW = b"o" * 32, b"n" * 32


async def test_insert_processes_is_idempotent(transformation_db: TransformationDB):
    a, b = b"a" * 32, b"b" * 32
    async with transformation_db as db:
        await db.insert_processes({a: {"class": "Workflow"}})
        await db.insert_processes({a: {"class": "Workflow"}, b: {"class": "Tool"}})
        await db.insert_processes({})
        count = await db.conn.scalar(select(func.count()).select_from(Processes))
    assert count == 2


async def test_get_processes_returns_only_what_is_stored(
    transformation_db: TransformationDB,
):
    a = b"a" * 32
    async with transformation_db as db:
        await db.insert_processes({a: {"class": "Workflow", "steps": {}}})
        found = await db.get_processes([a, b"z" * 32])
    assert found == {a: {"class": "Workflow", "steps": {}}}


async def add_transformation(db: TransformationDB, kind: str = "Compute"):
    """A paused transformation running OLD, with one parcel per state."""
    workgraph_id, ownership_id, transformation_id = uuid7(), uuid7(), uuid7()
    await db.conn.execute(
        insert(Workgraphs).values(
            workgraph_id=workgraph_id, type="MCSimulation", stripped_spec={}
        )
    )
    await db.conn.execute(
        insert(Ownership).values(
            ownership_id=ownership_id, vo="lhcb", sub="s", workgraph_id=workgraph_id
        )
    )
    await db.conn.execute(
        insert(Transformations).values(
            transformation_id=transformation_id,
            ownership_id=ownership_id,
            type="MCSimulation",
            kind=kind,
            status=TransformationStatus.PAUSED,
            packer="PerInput",
        )
    )
    parcels = {}
    if kind == "Compute":
        await db.conn.execute(
            insert(ComputeTransformations).values(
                transformation_id=transformation_id, process_id=OLD
            )
        )
        for status in ParcelStatus:
            parcels[status] = uuid7()
            await db.conn.execute(
                insert(Parcels).values(
                    parcel_id=parcels[status],
                    transformation_id=transformation_id,
                    kind="Compute",
                    status=status,
                )
            )
            await db.conn.execute(
                insert(ComputeParcels).values(parcel_id=parcels[status], process_id=OLD)
            )
    return transformation_id, parcels


@pytest.fixture
async def compute(transformation_db: TransformationDB):
    async with transformation_db as db:
        await db.insert_processes({OLD: {"class": "Workflow"}, NEW: {"class": "Tool"}})
        mine = await add_transformation(db)
        other = await add_transformation(db)
    return mine, other


async def test_lock_returns_status_and_process(
    transformation_db: TransformationDB, compute
):
    (transformation_id, _), _ = compute
    async with transformation_db as db:
        assert await db.lock_transformation_process(transformation_id) == (
            TransformationStatus.PAUSED,
            OLD,
        )
        assert await db.lock_transformation_process(uuid7()) is None
        data, _ = await add_transformation(db, kind="Data")
        assert await db.lock_transformation_process(data) is None


async def test_set_transformation_process_changes_only_that_transformation(
    transformation_db: TransformationDB, compute
):
    (mine, _), (other, _) = compute
    async with transformation_db as db:
        await db.set_transformation_process(mine, NEW)
        rows = dict(
            (
                await db.conn.execute(
                    select(
                        ComputeTransformations.transformation_id,
                        ComputeTransformations.process_id,
                    )
                )
            ).all()
        )
    assert rows == {mine: NEW, other: OLD}


async def test_only_unassigned_parcels_of_the_transformation_are_repointed(
    transformation_db: TransformationDB, compute
):
    (mine, parcels), (_, others) = compute
    async with transformation_db as db:
        assert await db.repoint_unassigned_parcels(mine, OLD, NEW) == 1
        assert await db.repoint_unassigned_parcels(mine, OLD, NEW) == 0
        rows = dict(
            (
                await db.conn.execute(
                    select(ComputeParcels.parcel_id, ComputeParcels.process_id)
                )
            ).all()
        )
    for status, parcel_id in parcels.items():
        assert rows[parcel_id] == (NEW if status == ParcelStatus.UNASSIGNED else OLD)
    assert all(rows[parcel_id] == OLD for parcel_id in others.values())


async def test_insert_transformation_log(transformation_db: TransformationDB, compute):
    (transformation_id, _), _ = compute
    async with transformation_db as db:
        await db.insert_transformation_log(
            transformation_id, "ProcessReplaced", "an-operator", data={"n": 1}
        )
        await db.insert_transformation_log(transformation_id, "Note", "someone")
        rows = (
            await db.conn.execute(
                select(
                    TransformationLog.event_type,
                    TransformationLog.author,
                    TransformationLog.message,
                    TransformationLog.data,
                ).order_by(TransformationLog.log_id)
            )
        ).all()
    assert [tuple(r) for r in rows] == [
        ("ProcessReplaced", "an-operator", None, {"n": 1}),
        ("Note", "someone", None, None),
    ]
