from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, event, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import (
    InputStatusConflictError,
    TransformationNotFoundError,
)
from diracx.core.models.transformation import (
    InputStatus,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.db import NewComputeParcel
from diracx.db.sql.transformation.schema import (
    COUNTER_STATUSES,
    ComputeParcels,
    ComputeTransformations,
    DataParcelDeltas,
    DataParcels,
    Inputs,
    Ownership,
    ParcelInputs,
    Parcels,
    Processes,
    RequirementsTemplates,
    TransformationLog,
    Transformations,
    Workgraphs,
)
from diracx.db.sql.utils.counters import counter_locks_table


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
    workgraph_id, ownership_id, transformation_id = uuid7(), uuid7(), uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Workgraphs).values(
                workgraph_id=workgraph_id, type="MCSimulation", stripped_spec={}
            )
        )
        await db.conn.execute(
            insert(Ownership).values(
                ownership_id=ownership_id,
                vo="lhcb",
                sub="some-subject",
                workgraph_id=workgraph_id,
            )
        )
        await db.conn.execute(
            insert(Transformations).values(
                transformation_id=transformation_id,
                ownership_id=ownership_id,
                type="MCSimulation",
                kind="Compute",
                packer="Standard",
            )
        )
    return transformation_id


async def test_post_create_seeds_counter_locks(transformation_db: TransformationDB):
    locks = counter_locks_table(transformation_db.metadata)
    async with transformation_db as db:
        # Running it again must not fail on the rows it already inserted
        await TransformationDB.post_create(db.conn)
        rows = (await db.conn.execute(select(locks))).all()
    assert sorted(rows) == [
        ("DataParcels", "fold"),
        ("DataParcels", "rebuild"),
        ("Transformation", "fold"),
        ("Transformation", "rebuild"),
    ]


async def test_transformation_round_trip(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        row = (
            await db.conn.execute(
                select(
                    Transformations.transformation_id,
                    Transformations.status,
                    Transformations.feeder_enabled,
                )
            )
        ).one()
    assert row == (transformation_id, TransformationStatus.NEW, True)

    # Clearing a JSON column stores SQL NULL, not the JSON literal null
    async with transformation_db as db:
        await db.conn.execute(update(Transformations).values(feeder_state={"a": 1}))
        await db.conn.execute(update(Transformations).values(feeder_state=None))
        cleared = await db.conn.scalar(
            select(func.count()).where(Transformations.feeder_state.is_(None))
        )
    assert cleared == 1


async def test_inputs_deduplicate_on_hash_not_lfn(
    transformation_db: TransformationDB, transformation_id: UUID
):
    parent_id = uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=parent_id,
                input_hash=b"\x01" * 16,
                lfn="/lhcb/file.dst",
                descriptor={"Run": 1234},
            )
        )
        # A split child shares the LFN of its parent but not its hash
        await db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=uuid7(),
                parent_input_id=parent_id,
                input_hash=b"\x02" * 16,
                lfn="/lhcb/file.dst",
                descriptor={"Run": 1234, "Mask": "1-10"},
            )
        )

    with pytest.raises(IntegrityError):
        async with transformation_db as db:
            await db.conn.execute(
                insert(Inputs).values(
                    transformation_id=transformation_id,
                    input_id=uuid7(),
                    input_hash=b"\x01" * 16,
                    lfn="/lhcb/file.dst",
                )
            )

    async with transformation_db as db:
        count = (
            await db.conn.execute(select(func.count()).select_from(Inputs))
        ).scalar()
    assert count == 2


async def test_deleting_a_split_parent_deletes_its_children(
    transformation_db: TransformationDB, transformation_id: UUID
):
    # MySQL checks foreign keys row by row, so the range delete that cleans a
    # transformation must not depend on deleting children before parents
    parent_id = uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=parent_id,
                input_hash=b"\x01" * 16,
                status=InputStatus.SPLIT,
            )
        )
        await db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=uuid7(),
                parent_input_id=parent_id,
                input_hash=b"\x02" * 16,
            )
        )

    async with transformation_db as db:
        await db.conn.execute(delete(Inputs).where(Inputs.input_id == parent_id))
        count = (
            await db.conn.execute(select(func.count()).select_from(Inputs))
        ).scalar()
    assert count == 0


async def test_data_parcel_deltas_are_never_negative(
    transformation_db: TransformationDB, transformation_id: UUID
):
    parcel_id = uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Parcels).values(
                parcel_id=parcel_id,
                transformation_id=transformation_id,
                kind=TransformationKind.DATA,
            )
        )
        await db.conn.execute(
            insert(DataParcels).values(parcel_id=parcel_id, request={})
        )
        await db.conn.execute(
            insert(DataParcelDeltas).values(
                parcel_id=parcel_id,
                storage_element="CERN-DST",
                files_added=1,
                bytes_added=100,
                files_freed=0,
                bytes_freed=0,
            )
        )

    with pytest.raises(IntegrityError):
        async with transformation_db as db:
            await db.conn.execute(
                insert(DataParcelDeltas).values(
                    parcel_id=parcel_id,
                    storage_element="CERN-BUFFER",
                    files_added=0,
                    bytes_added=0,
                    files_freed=1,
                    bytes_freed=-5,
                )
            )


def test_counter_statuses_cover_inputs_and_parcels():
    assert set(COUNTER_STATUSES) == {s.value for s in InputStatus} | {
        s.value for s in ParcelStatus
    }


# --------------------------------------------------------------------------
# Packing (#1083)
# --------------------------------------------------------------------------

PROCESS_ID = b"\x0a" * 32
TEMPLATE_ID = b"\x0b" * 32


@pytest.fixture
async def compute_facet(transformation_db: TransformationDB, transformation_id: UUID):
    async with transformation_db as db:
        await db.conn.execute(
            insert(Processes).values(process_id=PROCESS_ID, process={"class": "Tool"})
        )
        await db.conn.execute(
            insert(RequirementsTemplates).values(
                requirements_template_id=TEMPLATE_ID, requirements_template={"cpu": 1}
            )
        )
        await db.conn.execute(
            insert(ComputeTransformations).values(
                transformation_id=transformation_id,
                process_id=PROCESS_ID,
                requirements_template_id=TEMPLATE_ID,
            )
        )


async def add_inputs(
    db: TransformationDB, transformation_id: UUID, n: int, **values
) -> list[UUID]:
    input_ids = [uuid7() for _ in range(n)]
    await db.conn.execute(
        insert(Inputs),
        [
            {
                "TransformationID": transformation_id,
                "InputID": input_id,
                "InputHash": input_id.bytes,
                "LFN": f"/lhcb/{input_id}",
                "LFNSize": 10,
                "Descriptor": {"Run": 1},
                **values,
            }
            for input_id in input_ids
        ],
    )
    return input_ids


async def input_statuses(db: TransformationDB) -> dict[UUID, InputStatus]:
    return dict((await db.conn.execute(select(Inputs.input_id, Inputs.status))).all())


async def test_get_transformation(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        row = await db.get_transformation(transformation_id)
        assert row == {
            "status": TransformationStatus.NEW,
            "kind": TransformationKind.COMPUTE,
            "feeder": None,
            "feeder_enabled": True,
            "packer": "Standard",
            "packer_args": None,
        }
        with pytest.raises(TransformationNotFoundError):
            await db.get_transformation(uuid7())


async def test_list_transformation_ids(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        assert await db.list_transformation_ids(TransformationStatus.NEW) == [
            transformation_id
        ]
        assert await db.list_transformation_ids(TransformationStatus.ACTIVE) == []


async def test_input_overview(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        assert await db.get_input_overview(transformation_id) == {}
        await add_inputs(db, transformation_id, 3)
        await add_inputs(db, transformation_id, 2, Status=InputStatus.PROCESSED)
    async with transformation_db as db:
        assert await db.get_input_overview(transformation_id) == {
            InputStatus.UNASSIGNED: 3,
            InputStatus.PROCESSED: 2,
        }


async def test_claim_unassigned_inputs(
    transformation_db: TransformationDB, transformation_id: UUID
):
    now = datetime.now(tz=timezone.utc)
    async with transformation_db as db:
        packable = await add_inputs(db, transformation_id, 3)
        packable += await add_inputs(
            db, transformation_id, 1, DelayedUntil=now - timedelta(hours=1)
        )
        await add_inputs(
            db, transformation_id, 1, DelayedUntil=now + timedelta(hours=1)
        )
        await add_inputs(db, transformation_id, 1, Status=InputStatus.ASSIGNED)

    async with transformation_db as db:
        claimed = await db.claim_unassigned_inputs(
            transformation_id, now=now, limit=100
        )
        # Nothing of another transformation is claimed
        assert await db.claim_unassigned_inputs(uuid7(), now=now, limit=100) == []
    assert [i.input_id for i in claimed] == [uuid.UUID(bytes=i.bytes) for i in packable]
    assert all(type(i.input_id) is uuid.UUID for i in claimed)
    assert claimed[0].lfn == f"/lhcb/{packable[0]}"
    assert claimed[0].lfn_size == 10
    assert claimed[0].descriptor == {"Run": 1}

    # Paging by keyset
    async with transformation_db as db:
        first = await db.claim_unassigned_inputs(transformation_id, now=now, limit=2)
        second = await db.claim_unassigned_inputs(
            transformation_id, now=now, after_input_id=first[-1].input_id, limit=2
        )
    assert first + second == claimed


async def test_get_compute_facet(
    transformation_db: TransformationDB, transformation_id: UUID, compute_facet
):
    async with transformation_db as db:
        assert await db.get_compute_facet(transformation_id) == (
            PROCESS_ID,
            {"cpu": 1},
        )
        await db.conn.execute(
            update(ComputeTransformations).values(requirements_template_id=None)
        )
        assert await db.get_compute_facet(transformation_id) == (PROCESS_ID, {})
        with pytest.raises(TransformationNotFoundError):
            await db.get_compute_facet(uuid7())


async def test_insert_requirements_deduplicates(transformation_db: TransformationDB):
    async with transformation_db as db:
        await db.insert_requirements(b"\x01" * 32, {"cpu": 4})
    async with transformation_db as db:
        await db.insert_requirements(b"\x01" * 32, {"cpu": 4})
        count = await db.conn.scalar(
            select(func.count()).select_from(RequirementsTemplates)
        )
    assert count == 1


async def test_insert_compute_parcels(
    transformation_db: TransformationDB, transformation_id: UUID, compute_facet
):
    async with transformation_db as db:
        input_ids = await add_inputs(db, transformation_id, 3)
        leftover = await add_inputs(db, transformation_id, 1)

    parcels = [
        NewComputeParcel(uuid7(), TEMPLATE_ID, {"x": 1}, {"note": "a"}, input_ids[:2]),
        NewComputeParcel(uuid7(), None, {}, {}, input_ids[2:]),
    ]
    async with transformation_db as db:
        await db.insert_compute_parcels(transformation_id, PROCESS_ID, parcels)

    async with transformation_db as db:
        parcel_rows = (
            await db.conn.execute(
                select(
                    Parcels.parcel_id,
                    Parcels.kind,
                    Parcels.status,
                    Parcels.annotations,
                ).order_by(Parcels.parcel_id)
            )
        ).all()
        compute_rows = (
            await db.conn.execute(
                select(
                    ComputeParcels.parcel_id,
                    ComputeParcels.process_id,
                    ComputeParcels.requirements_template_id,
                    ComputeParcels.parameters,
                ).order_by(ComputeParcels.parcel_id)
            )
        ).all()
        links = set(
            (
                await db.conn.execute(
                    select(ParcelInputs.parcel_id, ParcelInputs.input_id)
                )
            ).all()
        )
        inputs = {
            row.InputID: row
            for row in await db.conn.execute(
                select(Inputs.input_id, Inputs.status, Inputs.last_parcel_id)
            )
        }

    (p1, p2) = parcels
    assert parcel_rows == [
        (
            p1.parcel_id,
            TransformationKind.COMPUTE,
            ParcelStatus.UNASSIGNED,
            {"note": "a"},
        ),
        (p2.parcel_id, TransformationKind.COMPUTE, ParcelStatus.UNASSIGNED, None),
    ]
    assert compute_rows == [
        (p1.parcel_id, PROCESS_ID, TEMPLATE_ID, {"x": 1}),
        (p2.parcel_id, PROCESS_ID, None, None),
    ]
    assert links == {
        (p1.parcel_id, input_ids[0]),
        (p1.parcel_id, input_ids[1]),
        (p2.parcel_id, input_ids[2]),
    }
    for parcel in parcels:
        for input_id in parcel.input_ids:
            assert inputs[input_id].Status == InputStatus.ASSIGNED
            assert inputs[input_id].LastParcelID == parcel.parcel_id
    assert inputs[leftover[0]].Status == InputStatus.UNASSIGNED
    assert inputs[leftover[0]].LastParcelID is None


async def test_insert_compute_parcels_refuses_assigned_inputs(
    transformation_db: TransformationDB, transformation_id: UUID, compute_facet
):
    async with transformation_db as db:
        input_ids = await add_inputs(
            db, transformation_id, 1, Status=InputStatus.ASSIGNED
        )
        input_ids += await add_inputs(db, transformation_id, 1)

    with pytest.raises(InputStatusConflictError):
        async with transformation_db as db:
            await db.insert_compute_parcels(
                transformation_id,
                PROCESS_ID,
                [NewComputeParcel(uuid7(), None, {}, {}, input_ids)],
            )

    # The whole transaction was rolled back
    async with transformation_db as db:
        assert await db.conn.scalar(select(func.count()).select_from(Parcels)) == 0
        statuses = await input_statuses(db)
    assert statuses[input_ids[1]] == InputStatus.UNASSIGNED


async def test_set_inputs_problematic(
    transformation_db: TransformationDB, transformation_id: UUID
):
    async with transformation_db as db:
        input_ids = await add_inputs(db, transformation_id, 2)

    async with transformation_db as db:
        await db.set_inputs_problematic(
            transformation_id, {input_ids[0]: "corrupt file"}, author="packer"
        )

    async with transformation_db as db:
        statuses = await input_statuses(db)
        logs = (
            await db.conn.execute(
                select(
                    TransformationLog.event_type,
                    TransformationLog.author,
                    TransformationLog.message,
                    TransformationLog.data,
                )
            )
        ).all()
    assert statuses == {
        input_ids[0]: InputStatus.PROBLEMATIC,
        input_ids[1]: InputStatus.UNASSIGNED,
    }
    assert logs == [
        (
            "InputProblematic",
            "packer",
            "corrupt file",
            {"InputID": str(input_ids[0]), "Reason": "corrupt file"},
        )
    ]

    with pytest.raises(InputStatusConflictError):
        async with transformation_db as db:
            await db.set_inputs_problematic(
                transformation_id, {input_ids[0]: "again"}, author="packer"
            )


async def test_delay_inputs(
    transformation_db: TransformationDB, transformation_id: UUID
):
    now = datetime.now(tz=timezone.utc)
    async with transformation_db as db:
        input_ids = await add_inputs(db, transformation_id, 2)

    async with transformation_db as db:
        await db.delay_inputs(
            transformation_id, input_ids[:1], now + timedelta(hours=1)
        )

    async with transformation_db as db:
        claimed = await db.claim_unassigned_inputs(transformation_id, now=now, limit=10)
        statuses = await input_statuses(db)
    assert [i.input_id for i in claimed] == [uuid.UUID(bytes=input_ids[1].bytes)]
    assert set(statuses.values()) == {InputStatus.UNASSIGNED}
