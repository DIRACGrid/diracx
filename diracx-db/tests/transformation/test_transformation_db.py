from __future__ import annotations

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, OperationalError
from uuid_utils import UUID, uuid7

from diracx.core.models.transformation import (
    InputStatus,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    COUNTER_STATUSES,
    DataParcelDeltas,
    DataParcels,
    Inputs,
    Ownership,
    Parcels,
    Transformations,
    Workgraphs,
)
from diracx.db.sql.utils.counters import counter_locks_table


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

    # SQLite reports a failed CHECK as an integrity error; MySQL and MariaDB,
    # through PyMySQL, as an operational one
    with pytest.raises(
        (IntegrityError, OperationalError), match="DataParcelDeltasNonNegative"
    ):
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
