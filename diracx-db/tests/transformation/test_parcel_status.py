"""Tests for the parcel queries and status transitions of the TransformationDB."""

from __future__ import annotations

import pytest
from sqlalchemy import event, insert, select
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


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        # SQLite only enforces foreign keys when asked to, per connection
        event.listen(
            db.engine.sync_engine,
            "connect",
            lambda dbapi_conn, _: dbapi_conn.cursor().execute("PRAGMA foreign_keys=ON"),
        )
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
            await db.post_create(conn)
        yield db


@pytest.fixture
async def transformation_id(transformation_db: TransformationDB) -> UUID:
    ownership_id, transformation_id = uuid7(), uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Ownership).values(
                ownership_id=ownership_id, vo="lhcb", sub="some-subject"
            )
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
    transformation_id: UUID | None,
    *,
    status: ParcelStatus = ParcelStatus.UNASSIGNED,
    kind: TransformationKind = TransformationKind.COMPUTE,
    backend: str | None = None,
    n_inputs: int = 0,
) -> UUID:
    """Insert a parcel, optionally made of ``n_inputs`` fresh Assigned inputs."""
    parcel_id = uuid7()
    await db.conn.execute(
        insert(Parcels).values(
            parcel_id=parcel_id,
            transformation_id=transformation_id,
            kind=kind,
            status=status,
            backend=backend,
        )
    )
    for _ in range(n_inputs):
        input_id = uuid7()
        await db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=input_id,
                input_hash=input_id.bytes,
                lfn=f"/lhcb/{input_id}",
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


async def test_get_parcel_ids_filters_and_orders(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        first = await add_parcel(db, transformation_id)
        second = await add_parcel(db, transformation_id)
        pinned = await add_parcel(db, transformation_id, backend="htcondor")
        await add_parcel(db, transformation_id, kind=TransformationKind.DATA)
        await add_parcel(db, transformation_id, status=ParcelStatus.DONE)

    async with transformation_db as db:
        compute = await db.get_parcel_ids(
            ParcelStatus.UNASSIGNED, kind=TransformationKind.COMPUTE
        )
        unconstrained = await db.get_parcel_ids(
            ParcelStatus.UNASSIGNED,
            kind=TransformationKind.COMPUTE,
            backends=[None, "dummy"],
        )
        limited = await db.get_parcel_ids(ParcelStatus.UNASSIGNED, limit=1)

    # UUIDv7 key order is creation order
    assert compute == [first, second, pinned]
    assert unconstrained == [first, second]
    assert limited == [first]


async def test_set_parcel_status_is_compare_and_set(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        unassigned = await add_parcel(db, transformation_id)
        already_reserved = await add_parcel(
            db, transformation_id, status=ParcelStatus.RESERVED
        )

    async with transformation_db as db:
        moved = await db.set_parcel_status(
            [unassigned, already_reserved, uuid7()],
            ParcelStatus.UNASSIGNED,
            ParcelStatus.RESERVED,
            backend="dummy",
        )
        # A second claim of the same parcels finds nothing left to move
        moved_again = await db.set_parcel_status(
            [unassigned], ParcelStatus.UNASSIGNED, ParcelStatus.RESERVED
        )
        rows = (
            await db.conn.execute(
                select(Parcels.parcel_id, Parcels.status, Parcels.backend)
            )
        ).all()

    assert moved == [unassigned]
    assert moved_again == []
    assert sorted(rows) == sorted(
        [
            (unassigned, ParcelStatus.RESERVED, "dummy"),
            (already_reserved, ParcelStatus.RESERVED, None),
        ]
    )


async def test_final_status_stamps_completion_time(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        parcel_id = await add_parcel(
            db, transformation_id, status=ParcelStatus.ASSIGNED
        )

    async with transformation_db as db:
        await db.set_parcel_status(
            [parcel_id], ParcelStatus.ASSIGNED, ParcelStatus.COMPLETING
        )
        assert await db.conn.scalar(select(Parcels.completed_at)) is None
        await db.set_parcel_status(
            [parcel_id], ParcelStatus.COMPLETING, ParcelStatus.DONE
        )
        assert await db.conn.scalar(select(Parcels.completed_at)) is not None


async def test_user_parcels_can_move(transformation_db: TransformationDB):
    """User parcels have no transformation."""
    async with transformation_db as db:
        parcel_id = await add_parcel(db, None)

    async with transformation_db as db:
        moved = await db.set_parcel_status(
            [parcel_id], ParcelStatus.UNASSIGNED, ParcelStatus.RESERVED
        )

    assert moved == [parcel_id]


async def test_set_parcel_input_status(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        parcel_id = await add_parcel(
            db, transformation_id, status=ParcelStatus.ASSIGNED, n_inputs=3
        )
        other_parcel_id = await add_parcel(
            db, transformation_id, status=ParcelStatus.ASSIGNED, n_inputs=1
        )

    async with transformation_db as db:
        moved = await db.set_parcel_input_status(
            [parcel_id],
            InputStatus.ASSIGNED,
            InputStatus.FAILED,
            increment_error_count=True,
        )
        rows = (
            await db.conn.execute(
                select(Inputs.last_parcel_id, Inputs.status, Inputs.error_count)
            )
        ).all()

    assert moved == 3
    assert sorted(rows) == sorted(
        [(parcel_id, InputStatus.FAILED, 1)] * 3
        + [(other_parcel_id, InputStatus.ASSIGNED, 0)]
    )


async def test_empty_transitions_do_nothing(transformation_db: TransformationDB):
    async with transformation_db as db:
        assert (
            await db.set_parcel_status(
                [], ParcelStatus.UNASSIGNED, ParcelStatus.RESERVED
            )
            == []
        )
        assert (
            await db.set_parcel_input_status(
                [], InputStatus.ASSIGNED, InputStatus.PROCESSED
            )
            == 0
        )
