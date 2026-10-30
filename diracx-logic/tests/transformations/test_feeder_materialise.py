"""Tests for feeder yield materialisation (DX-ADR-006)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import event, func, insert, select
from uuid_utils import UUID, uuid7

from diracx.core.hashing import compute_input_hash
from diracx.core.models.feeder import Input, NoMoreInputs, SetFeederState
from diracx.core.models.transformation import (
    InputStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import Inputs, Ownership, Transformations
from diracx.logic.transformations import materialise_feeder_yields


@pytest.fixture
async def transformation_db():
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


@pytest.fixture
async def transformation_id(transformation_db: TransformationDB) -> UUID:
    ownership_id, transformation_id = uuid7(), uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Ownership).values(
                ownership_id=ownership_id,
                vo="lhcb",
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
                feeder_args={"max_count": 10},
                feeder_state={"next_seed": 0},
                feeder_enabled=True,
                packer="Standard",
            )
        )
    return transformation_id


def _fake_seed_feeder(*, start: int, stop: int, exhausted: bool = False):
    """Yield seeds in one batch, then optionally signal exhaustion."""
    for seed in range(start, stop):
        yield Input(descriptor={"seed": seed})
    yield SetFeederState(state={"next_seed": stop})
    if exhausted:
        yield NoMoreInputs()


async def test_materialise_commits_batch_with_feeder_state(
    transformation_db: TransformationDB, transformation_id: UUID
):
    await materialise_feeder_yields(
        transformation_id,
        _fake_seed_feeder(start=0, stop=3),
        transformation_db,
    )

    async with transformation_db as db:
        count = await db.conn.scalar(
            select(func.count()).where(Inputs.transformation_id == transformation_id)
        )
        columns = await db.get_feeder_columns(transformation_id)
        rows = (
            await db.conn.execute(
                select(
                    Inputs.descriptor.label("descriptor"),
                    Inputs.status.label("status"),
                    Inputs.input_hash.label("input_hash"),
                ).where(Inputs.transformation_id == transformation_id)
            )
        ).all()

    assert count == 3
    assert columns is not None
    assert columns["feeder_state"] == {"next_seed": 3}
    assert columns["feeder_enabled"] is True
    assert {row.descriptor["seed"] for row in rows} == {0, 1, 2}
    assert all(row.status == InputStatus.UNASSIGNED for row in rows)
    assert {row.input_hash for row in rows} == {
        compute_input_hash(None, {"seed": 0}),
        compute_input_hash(None, {"seed": 1}),
        compute_input_hash(None, {"seed": 2}),
    }


async def test_materialise_no_more_inputs_disables_feeder(
    transformation_db: TransformationDB, transformation_id: UUID
):
    await materialise_feeder_yields(
        transformation_id,
        _fake_seed_feeder(start=0, stop=2, exhausted=True),
        transformation_db,
    )

    async with transformation_db as db:
        count = await db.conn.scalar(
            select(func.count()).where(Inputs.transformation_id == transformation_id)
        )
        columns = await db.get_feeder_columns(transformation_id)

    assert count == 2
    assert columns is not None
    assert columns["feeder_state"] == {"next_seed": 2}
    assert columns["feeder_enabled"] is False


async def test_materialise_no_more_inputs_flushes_pending_without_state(
    transformation_db: TransformationDB, transformation_id: UUID
):
    delayed = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)

    def _pending_then_done():
        yield Input(
            lfn="/lhcb/a.dst",
            lfn_size=10,
            descriptor={"Mask": "1"},
            delayed_until=delayed,
        )
        yield NoMoreInputs()

    await materialise_feeder_yields(
        transformation_id, _pending_then_done(), transformation_db
    )

    async with transformation_db as db:
        row = (
            await db.conn.execute(
                select(
                    Inputs.lfn.label("lfn"),
                    Inputs.lfn_size.label("lfn_size"),
                    Inputs.delayed_until.label("delayed_until"),
                    Inputs.descriptor.label("descriptor"),
                ).where(Inputs.transformation_id == transformation_id)
            )
        ).one()
        columns = await db.get_feeder_columns(transformation_id)

    assert row.lfn == "/lhcb/a.dst"
    assert row.lfn_size == 10
    assert row.delayed_until == delayed
    assert row.descriptor == {"Mask": "1"}
    assert columns is not None
    assert columns["feeder_state"] == {"next_seed": 0}
    assert columns["feeder_enabled"] is False


async def test_materialise_deduplicates_on_refeed(
    transformation_db: TransformationDB, transformation_id: UUID
):
    await materialise_feeder_yields(
        transformation_id,
        _fake_seed_feeder(start=0, stop=2),
        transformation_db,
    )
    # Same hashes again must not raise or create extra rows
    await materialise_feeder_yields(
        transformation_id,
        _fake_seed_feeder(start=0, stop=2),
        transformation_db,
    )

    async with transformation_db as db:
        count = await db.conn.scalar(
            select(func.count()).where(Inputs.transformation_id == transformation_id)
        )

    assert count == 2


async def test_materialise_flushes_remainder_without_feeder_state(
    transformation_db: TransformationDB, transformation_id: UUID
):
    await materialise_feeder_yields(
        transformation_id,
        [Input(descriptor={"seed": 1})],
        transformation_db,
    )

    async with transformation_db as db:
        count = await db.conn.scalar(
            select(func.count()).where(Inputs.transformation_id == transformation_id)
        )
        columns = await db.get_feeder_columns(transformation_id)

    assert count == 1
    assert columns is not None
    assert columns["feeder_state"] == {"next_seed": 0}
    assert columns["feeder_enabled"] is True


async def test_materialise_flushes_full_input_batches_before_state(
    transformation_db: TransformationDB, transformation_id: UUID
):
    def _gen():
        for seed in range(5):
            yield Input(descriptor={"seed": seed})
        raise RuntimeError("stop before SetFeederState")

    with pytest.raises(RuntimeError, match="stop before SetFeederState"):
        await materialise_feeder_yields(
            transformation_id,
            _gen(),
            transformation_db,
            input_batch_size=2,
        )

    async with transformation_db as db:
        count = await db.conn.scalar(
            select(func.count()).where(Inputs.transformation_id == transformation_id)
        )
        columns = await db.get_feeder_columns(transformation_id)

    # Two full batches of 2 committed; the 5th Input was still pending.
    assert count == 4
    assert columns is not None
    assert columns["feeder_state"] == {"next_seed": 0}


async def test_materialise_rejects_invalid_batch_size(
    transformation_db: TransformationDB, transformation_id: UUID
):
    with pytest.raises(ValueError, match="input_batch_size"):
        await materialise_feeder_yields(
            transformation_id,
            [],
            transformation_db,
            input_batch_size=0,
        )


async def test_materialise_rejects_unknown_yield_type(
    transformation_db: TransformationDB, transformation_id: UUID
):
    with pytest.raises(TypeError, match="unsupported feeder yield type"):
        await materialise_feeder_yields(
            transformation_id,
            ["not-a-proposal"],  # type: ignore[list-item]
            transformation_db,
        )
