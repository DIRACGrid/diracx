from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, event, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from uuid_utils import UUID, uuid7

from diracx.core.hashing import compute_input_hash
from diracx.core.models.transformation import (
    InputStatus,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformations.schema import (
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


@pytest.fixture
async def standalone_active_transformation(
    transformation_db: TransformationDB,
) -> UUID:
    """Active standalone transformation with a feeder (no workgraph)."""
    ownership_id, transformation_id = uuid7(), uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Ownership).values(
                ownership_id=ownership_id,
                vo="cms",
                sub="feeder-owner",
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
    return transformation_id


async def test_list_feedable_transformation_ids(
    transformation_db: TransformationDB,
    standalone_active_transformation: UUID,
    transformation_id: UUID,
):
    # Existing fixture is New with no Feeder; standalone is Active+enabled
    async with transformation_db as db:
        feedable = await db.list_feedable_transformation_ids()
    assert feedable == [standalone_active_transformation]

    async with transformation_db as db:
        await db.set_feeder_enabled(standalone_active_transformation, False)
        assert await db.list_feedable_transformation_ids() == []

        await db.set_feeder_enabled(standalone_active_transformation, True)
        await db.conn.execute(
            update(Transformations)
            .where(
                Transformations.transformation_id == standalone_active_transformation
            )
            .values(feeder=None)
        )
        assert await db.list_feedable_transformation_ids() == []

        # Non-Active with feeder set is still excluded
        await db.conn.execute(
            update(Transformations)
            .where(Transformations.transformation_id == transformation_id)
            .values(
                feeder="IntegerFeeder",
                feeder_enabled=True,
                status=TransformationStatus.NEW,
            )
        )
        assert await db.list_feedable_transformation_ids() == []


async def test_get_and_set_feeder_columns(
    transformation_db: TransformationDB, standalone_active_transformation: UUID
):
    async with transformation_db as db:
        columns = await db.get_feeder_columns(standalone_active_transformation)
        assert columns == {
            "feeder": "IntegerFeeder",
            "feeder_args": {"max_count": 3},
            "feeder_state": {"next_seed": 0},
            "feeder_enabled": True,
        }

        await db.set_feeder_state(standalone_active_transformation, {"next_seed": 7})
        await db.set_feeder_enabled(standalone_active_transformation, False)
        columns = await db.get_feeder_columns(standalone_active_transformation)
        assert columns == {
            "feeder": "IntegerFeeder",
            "feeder_args": {"max_count": 3},
            "feeder_state": {"next_seed": 7},
            "feeder_enabled": False,
        }

        await db.set_feeder_state(standalone_active_transformation, None)
        columns = await db.get_feeder_columns(standalone_active_transformation)
        assert columns is not None
        assert columns["feeder_state"] is None

        assert await db.get_feeder_columns(uuid7()) is None


async def test_insert_unassigned_inputs_deduplicates_on_hash(
    transformation_db: TransformationDB, standalone_active_transformation: UUID
):
    input_id_a, input_id_b, input_id_dup = uuid7(), uuid7(), uuid7()
    descriptor_a = {"seed": 1}
    descriptor_b = {"seed": 2}
    delayed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    hash_a = compute_input_hash(None, descriptor_a)
    hash_b = compute_input_hash(None, descriptor_b)

    async with transformation_db as db:
        await db.insert_unassigned_inputs(standalone_active_transformation, [])
        await db.insert_unassigned_inputs(
            standalone_active_transformation,
            [
                {
                    "input_id": input_id_a,
                    "input_hash": hash_a,
                    "descriptor": descriptor_a,
                    "delayed_until": delayed,
                },
                {
                    "input_id": input_id_b,
                    "input_hash": hash_b,
                    "descriptor": descriptor_b,
                },
            ],
        )
        # Same hash again must be ignored, not raise
        await db.insert_unassigned_inputs(
            standalone_active_transformation,
            [
                {
                    "input_id": input_id_dup,
                    "input_hash": hash_a,
                    "descriptor": descriptor_a,
                }
            ],
        )

        rows = (
            await db.conn.execute(
                select(
                    Inputs.input_id.label("input_id"),
                    Inputs.input_hash.label("input_hash"),
                    Inputs.descriptor.label("descriptor"),
                    Inputs.status.label("status"),
                    Inputs.delayed_until.label("delayed_until"),
                    Inputs.lfn.label("lfn"),
                ).where(Inputs.transformation_id == standalone_active_transformation)
            )
        ).all()

    assert len(rows) == 2
    by_id = {row.input_id: row for row in rows}
    assert by_id[input_id_a].input_hash == hash_a
    assert by_id[input_id_a].descriptor == descriptor_a
    assert by_id[input_id_a].status == InputStatus.UNASSIGNED
    assert by_id[input_id_a].delayed_until == delayed
    assert by_id[input_id_a].lfn is None
    assert by_id[input_id_b].input_hash == hash_b
    assert input_id_dup not in by_id
