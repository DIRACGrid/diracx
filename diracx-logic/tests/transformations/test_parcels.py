"""Tests for the parcel state machine of diracx.logic.transformations."""

from __future__ import annotations

import pytest
from sqlalchemy import insert, select
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import InvalidParcelTransitionError
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
from diracx.logic.transformations import set_parcel_status


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
            await db.post_create(conn)
        yield db


async def add_assigned_parcel(db: TransformationDB, n_inputs: int) -> UUID:
    """Insert a transformation with one Assigned parcel made of ``n_inputs``."""
    ownership_id, transformation_id, parcel_id = uuid7(), uuid7(), uuid7()
    await db.conn.execute(
        insert(Ownership).values(ownership_id=ownership_id, vo="lhcb", sub="someone")
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
    await db.conn.execute(
        insert(Parcels).values(
            parcel_id=parcel_id,
            transformation_id=transformation_id,
            kind=TransformationKind.COMPUTE,
            status=ParcelStatus.ASSIGNED,
        )
    )
    for _ in range(n_inputs):
        input_id = uuid7()
        await db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=input_id,
                input_hash=input_id.bytes,
                status=InputStatus.ASSIGNED,
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


async def input_states(db: TransformationDB) -> list[tuple[InputStatus, int]]:
    return sorted(
        (await db.conn.execute(select(Inputs.status, Inputs.error_count))).all()
    )


@pytest.mark.parametrize(
    "from_status, to_status",
    [
        # Skipping a state
        (ParcelStatus.UNASSIGNED, ParcelStatus.ASSIGNED),
        (ParcelStatus.ASSIGNED, ParcelStatus.DONE),
        # Leaving a final state: parcels are not retryable
        (ParcelStatus.DONE, ParcelStatus.UNASSIGNED),
        (ParcelStatus.FAILED, ParcelStatus.COMPLETING),
    ],
)
async def test_forbidden_transitions_are_refused(
    transformation_db: TransformationDB, from_status, to_status
):
    async with transformation_db as db:
        with pytest.raises(InvalidParcelTransitionError):
            await set_parcel_status(
                [uuid7()], from_status, to_status, transformation_db=db
            )


async def test_duplicate_parcel_ids_are_refused(transformation_db: TransformationDB):
    async with transformation_db as db:
        parcel_id = await add_assigned_parcel(db, n_inputs=1)
        with pytest.raises(ValueError, match="only once"):
            await set_parcel_status(
                [parcel_id, parcel_id],
                ParcelStatus.ASSIGNED,
                ParcelStatus.COMPLETING,
                transformation_db=db,
            )
        # Nothing was moved
        assert await db.conn.scalar(select(Parcels.status)) == ParcelStatus.ASSIGNED


async def test_partially_done_is_not_implemented(transformation_db: TransformationDB):
    async with transformation_db as db:
        with pytest.raises(NotImplementedError):
            await set_parcel_status(
                [uuid7()],
                ParcelStatus.COMPLETING,
                ParcelStatus.PARTIALLY_DONE,
                transformation_db=db,
            )


async def test_non_final_transition_leaves_inputs_alone(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        parcel_id = await add_assigned_parcel(db, n_inputs=2)
        moved = await set_parcel_status(
            [parcel_id],
            ParcelStatus.ASSIGNED,
            ParcelStatus.COMPLETING,
            transformation_db=db,
        )
        assert moved == [parcel_id]
        assert await input_states(db) == [(InputStatus.ASSIGNED, 0)] * 2


@pytest.mark.parametrize(
    "to_status, expected_input_state",
    [
        (ParcelStatus.DONE, (InputStatus.PROCESSED, 0)),
        (ParcelStatus.FAILED, (InputStatus.FAILED, 1)),
        (ParcelStatus.CANCELLED, (InputStatus.NOT_PROCESSED, 0)),
    ],
)
async def test_final_status_moves_inputs(
    transformation_db: TransformationDB, to_status, expected_input_state
):
    async with transformation_db as db:
        parcel_id = await add_assigned_parcel(db, n_inputs=2)
        await set_parcel_status(
            [parcel_id],
            ParcelStatus.ASSIGNED,
            ParcelStatus.COMPLETING,
            transformation_db=db,
        )
        await set_parcel_status(
            [parcel_id], ParcelStatus.COMPLETING, to_status, transformation_db=db
        )
        assert await input_states(db) == [expected_input_state] * 2


async def test_parcels_not_moved_do_not_move_inputs(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        parcel_id = await add_assigned_parcel(db, n_inputs=1)
        # The parcel is Assigned, not Completing, so nothing matches
        moved = await set_parcel_status(
            [parcel_id],
            ParcelStatus.COMPLETING,
            ParcelStatus.DONE,
            transformation_db=db,
        )
        assert moved == []
        assert await input_states(db) == [(InputStatus.ASSIGNED, 0)]
