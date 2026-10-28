"""Tests for the parcel queries and status transitions of the TransformationDB."""

from __future__ import annotations

import pytest
from sqlalchemy import event, insert, select
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models.search import (
    ScalarSearchOperator,
    SearchSpec,
    SortDirection,
    SortSpec,
)
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


def unassigned(kind: TransformationKind) -> list[SearchSpec]:
    return [
        {
            "parameter": "Status",
            "operator": ScalarSearchOperator.EQUAL,
            "value": ParcelStatus.UNASSIGNED,
        },
        {"parameter": "Kind", "operator": ScalarSearchOperator.EQUAL, "value": kind},
    ]


OLDEST_FIRST: list[SortSpec] = [
    {"parameter": "ParcelID", "direction": SortDirection.ASC}
]


async def test_search_parcels_filters_and_orders(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        first = await add_parcel(db, transformation_id)
        second = await add_parcel(db, transformation_id)
        pinned = await add_parcel(db, transformation_id, backend="htcondor")
        dummy = await add_parcel(db, transformation_id, backend="dummy")
        await add_parcel(db, transformation_id, kind=TransformationKind.DATA)
        await add_parcel(db, transformation_id, status=ParcelStatus.DONE)

    async with transformation_db as db:
        compute = await db.search_parcels(
            ["ParcelID"], unassigned(TransformationKind.COMPUTE), OLDEST_FIRST
        )
        unconstrained = await db.search_parcels(
            ["ParcelID"],
            unassigned(TransformationKind.COMPUTE),
            OLDEST_FIRST,
            backends=[None, "dummy"],
        )
        only_dummy = await db.search_parcels(
            ["ParcelID"],
            unassigned(TransformationKind.COMPUTE),
            OLDEST_FIRST,
            backends=["dummy"],
        )
        limited = await db.search_parcels(
            ["ParcelID", "Backend"],
            unassigned(TransformationKind.COMPUTE),
            OLDEST_FIRST,
            backends=[None, "dummy"],
            per_page=1,
            page=2,
        )

    def ids(result):
        return [row["ParcelID"] for row in result[1]]

    # UUIDv7 key order is creation order
    assert compute[0] == 4
    assert ids(compute) == [first, second, pinned, dummy]
    assert ids(unconstrained) == [first, second, dummy]
    assert ids(only_dummy) == [dummy]
    # The total counts every match, the page holds only the requested rows
    assert limited == (3, [{"ParcelID": second, "Backend": None}])


async def test_search_parcels_rejects_bad_pages(transformation_db: TransformationDB):
    async with transformation_db as db:
        for page, per_page in [(0, 10), (1, 0)]:
            with pytest.raises(InvalidQueryError):
                await db.search_parcels(None, [], [], per_page=per_page, page=page)


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
