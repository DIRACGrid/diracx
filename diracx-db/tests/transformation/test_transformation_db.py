from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, OperationalError
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import (
    InputStatusConflictError,
    TransformationNotFoundError,
)
from diracx.core.hashing import hash_json
from diracx.core.models.transformation import (
    FeederInput,
    InputStatus,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.db import NewComputeParcel, _input_hash
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

    # SQLite reports CHECK violations as IntegrityError, but PyMySQL maps the
    # MySQL (3819) and MariaDB (4025) errors to OperationalError
    with pytest.raises((IntegrityError, OperationalError)):
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
            "feeder_args": None,
            "feeder_state": None,
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


# --------------------------------------------------------------------------
# Feeder (#1082)
# --------------------------------------------------------------------------


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


async def test_set_feeder_state_and_enabled(
    transformation_db: TransformationDB, standalone_active_transformation: UUID
):
    async with transformation_db as db:
        row = await db.get_transformation(standalone_active_transformation)
        assert row["feeder"] == "IntegerFeeder"
        assert row["feeder_args"] == {"max_count": 3}
        assert row["feeder_state"] == {"next_seed": 0}
        assert row["feeder_enabled"] is True

        await db.set_feeder_state(standalone_active_transformation, {"next_seed": 7})
        await db.set_feeder_enabled(standalone_active_transformation, False)
        row = await db.get_transformation(standalone_active_transformation)
        assert row["feeder_args"] == {"max_count": 3}
        assert row["feeder_state"] == {"next_seed": 7}
        assert row["feeder_enabled"] is False

        await db.set_feeder_state(standalone_active_transformation, None)
        row = await db.get_transformation(standalone_active_transformation)
        assert row["feeder_state"] is None


def test_input_hash_length_and_stability():
    digest = _input_hash("/lhcb/file.dst", {"Run": 1234, "Mask": "1-10"})
    assert len(digest) == 16
    assert digest == _input_hash("/lhcb/file.dst", {"Mask": "1-10", "Run": 1234})

    other = _input_hash("/lhcb/file.dst", {"Run": 1234, "Mask": "11-20"})
    assert digest != other

    null_lfn = _input_hash(None, {"Seed": 42})
    assert len(null_lfn) == 16
    assert null_lfn != _input_hash(None, {"Seed": 43})

    null_mask = _input_hash("/cms/file1.root", None)
    dict_mask = _input_hash("/cms/file1.root", {})
    assert null_mask == dict_mask


def test_input_hash_none_descriptor_matches_empty():
    assert _input_hash("/a", None) == _input_hash("/a", {})
    assert _input_hash(None, None) == _input_hash(None, {})


def test_input_hash_rejects_integer_outside_jcs_domain():
    # JCS only accepts IEEE-754 safe integers (about ±2**53);
    # rfc8785.IntegerDomainError is a ValueError.
    with pytest.raises(ValueError):
        _input_hash(None, {"Seed": 2**63 - 1})
    with pytest.raises(ValueError):
        _input_hash(None, {"Seed": -(2**63)})


def test_input_hash_matches_truncated_hash_json_over_envelope():
    lfn = None
    descriptor = {"Seed": 1}
    envelope = {"LFN": lfn, "Descriptor": descriptor}
    expected = hash_json(envelope)[:16]
    assert _input_hash(lfn, descriptor) == expected
    assert len(expected) == 16


async def test_insert_unassigned_inputs_deduplicates_on_hash(
    transformation_db: TransformationDB, standalone_active_transformation: UUID
):
    descriptor_a = {"seed": 1}
    descriptor_b = {"seed": 2}
    delayed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    hash_a = _input_hash(None, descriptor_a)
    hash_b = _input_hash(None, descriptor_b)
    proposal_a = FeederInput(descriptor=descriptor_a, delayed_until=delayed)
    proposal_b = FeederInput(descriptor=descriptor_b)

    async with transformation_db as db:
        assert (
            await db.insert_unassigned_inputs(standalone_active_transformation, [])
            == []
        )
        inserted = await db.insert_unassigned_inputs(
            standalone_active_transformation,
            [proposal_a, proposal_b, proposal_a],
        )
        assert len(inserted) == 2
        # Same (lfn, descriptor) again must insert nothing
        assert (
            await db.insert_unassigned_inputs(
                standalone_active_transformation, [proposal_a]
            )
            == []
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
    by_hash = {row.input_hash: row for row in rows}
    assert set(by_hash) == {hash_a, hash_b}
    assert {row.input_id for row in rows} == set(inserted)
    assert by_hash[hash_a].descriptor == descriptor_a
    assert by_hash[hash_a].status == InputStatus.UNASSIGNED
    assert by_hash[hash_a].delayed_until == delayed
    assert by_hash[hash_a].lfn is None
    assert by_hash[hash_b].descriptor == descriptor_b


async def test_insert_unassigned_inputs_dedupes_across_chunks(
    transformation_db: TransformationDB, standalone_active_transformation: UUID
):
    proposals = [
        FeederInput(descriptor={"seed": 1}),
        FeederInput(descriptor={"seed": 2}),
        FeederInput(descriptor={"seed": 1}),
        FeederInput(descriptor={"seed": 3}),
    ]

    async with transformation_db as db:
        with pytest.raises(ValueError):
            await db.insert_unassigned_inputs(
                standalone_active_transformation, proposals, chunk_size=0
            )
        # insert 3 out of 4 files (one is duplicated)
        inserted = await db.insert_unassigned_inputs(
            standalone_active_transformation, proposals, chunk_size=1
        )
        assert len(inserted) == 3
        # insert 0 out of 4 files (all are duplicated)
        inserted = await db.insert_unassigned_inputs(
            standalone_active_transformation, proposals, chunk_size=2
        )
        assert len(inserted) == 0
        # check that 3 files are in the Inputs table
        count = (
            await db.conn.execute(
                select(func.count())
                .select_from(Inputs)
                .where(Inputs.transformation_id == standalone_active_transformation)
            )
        ).scalar_one()
    assert count == 3


async def test_insert_unassigned_inputs_accepts_generator(
    transformation_db: TransformationDB, standalone_active_transformation: UUID
):
    # A one-shot iterator is consumed chunk by chunk, keeping first-seen order
    proposals = (FeederInput(descriptor={"seed": seed}) for seed in [1, 2, 1, 3, 4])

    async with transformation_db as db:
        inserted = await db.insert_unassigned_inputs(
            standalone_active_transformation, proposals, chunk_size=2
        )
        assert len(inserted) == 4
        rows = (
            await db.conn.execute(
                select(Inputs.input_id, Inputs.descriptor).where(
                    Inputs.input_id.in_(inserted)
                )
            )
        ).all()
    descriptor_by_id = {input_id: descriptor for input_id, descriptor in rows}
    assert [descriptor_by_id[i]["seed"] for i in inserted] == [1, 2, 3, 4]


async def test_insert_unassigned_inputs_rejects_invalid_feeder_input(
    transformation_db: TransformationDB, standalone_active_transformation: UUID
):
    with pytest.raises(ValidationError):
        FeederInput(lfn="/lhcb/a.dst")

    async with transformation_db as db:
        inserted = await db.insert_unassigned_inputs(
            standalone_active_transformation, [FeederInput()]
        )
        assert len(inserted) == 1
        descriptor = (
            await db.conn.execute(
                select(Inputs.descriptor).where(
                    Inputs.transformation_id == standalone_active_transformation,
                    Inputs.input_id == inserted[0],
                )
            )
        ).scalar_one()
    assert descriptor == {}
