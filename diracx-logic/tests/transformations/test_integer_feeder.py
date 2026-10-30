"""Tests for the core IntegerFeeder plugin."""

from __future__ import annotations

import pytest
from sqlalchemy import event, func, insert, select
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import ConfigurationError
from diracx.core.models.feeder import Input, NoMoreInputs, SetFeederState
from diracx.core.models.transformation import (
    InputStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import Inputs, Ownership, Transformations
from diracx.logic.transformations import (
    IntegerFeederArgs,
    integer_feeder,
    load_feeder,
    materialise_feeder_yields,
)


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


async def _insert_integer_transformation(
    transformation_db: TransformationDB,
    *,
    max_count: int,
    next_seed: int = 0,
    feeder_enabled: bool = True,
) -> UUID:
    ownership_id, transformation_id = uuid7(), uuid7()
    async with transformation_db as db:
        await db.conn.execute(
            insert(Ownership).values(
                ownership_id=ownership_id,
                vo="lhcb",
                sub="integer-feeder-owner",
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
                feeder_args={"max_count": max_count},
                feeder_state={"next_seed": next_seed},
                feeder_enabled=feeder_enabled,
                packer="Standard",
            )
        )
    return transformation_id


def test_integer_feeder_yields_seeds_then_exhausts():
    yields = list(
        integer_feeder(
            feeder_args=IntegerFeederArgs(max_count=3),
            feeder_state={"next_seed": 0},
            scouting=False,
        )
    )
    assert [type(item) for item in yields] == [
        Input,
        Input,
        Input,
        SetFeederState,
        NoMoreInputs,
    ]
    assert [item.descriptor for item in yields[:3]] == [
        {"seed": 0},
        {"seed": 1},
        {"seed": 2},
    ]
    assert yields[3].state == {"next_seed": 3}


def test_integer_feeder_resumes_from_bookmark():
    yields = list(
        integer_feeder(
            feeder_args=IntegerFeederArgs(max_count=4),
            feeder_state={"next_seed": 2},
            scouting=False,
        )
    )
    assert [item.descriptor["seed"] for item in yields if isinstance(item, Input)] == [
        2,
        3,
    ]
    assert yields[-2].state == {"next_seed": 4}
    assert isinstance(yields[-1], NoMoreInputs)


def test_integer_feeder_already_exhausted():
    yields = list(
        integer_feeder(
            feeder_args=IntegerFeederArgs(max_count=2),
            feeder_state={"next_seed": 2},
            scouting=False,
        )
    )
    assert yields == [NoMoreInputs()]


def test_load_feeder_resolves_integer_feeder_entry_point():
    plugin, args_model = load_feeder("IntegerFeeder")
    assert plugin is integer_feeder
    assert args_model is IntegerFeederArgs


def test_load_feeder_unknown_name():
    with pytest.raises(ConfigurationError, match="No feeder named"):
        load_feeder("DoesNotExistFeeder")


async def test_integer_feeder_materialises_and_disables(
    transformation_db: TransformationDB,
):
    transformation_id = await _insert_integer_transformation(
        transformation_db, max_count=5
    )
    plugin, args_model = load_feeder("IntegerFeeder")
    async with transformation_db as db:
        columns = await db.get_feeder_columns(transformation_id)
    assert columns is not None
    yields = plugin(
        feeder_args=args_model.model_validate(columns["feeder_args"]),
        feeder_state=columns["feeder_state"] or {},
        scouting=False,
    )
    await materialise_feeder_yields(transformation_id, yields, transformation_db)

    async with transformation_db as db:
        count = await db.conn.scalar(
            select(func.count()).where(Inputs.transformation_id == transformation_id)
        )
        status = await db.conn.scalar(
            select(Inputs.status).where(Inputs.transformation_id == transformation_id)
        )
        columns = await db.get_feeder_columns(transformation_id)

    assert count == 5
    assert status == InputStatus.UNASSIGNED
    assert columns is not None
    assert columns["feeder_state"] == {"next_seed": 5}
    assert columns["feeder_enabled"] is False
