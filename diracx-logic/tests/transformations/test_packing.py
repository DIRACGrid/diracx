from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import event, insert, select, update
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import ConfigurationError
from diracx.core.models.transformation import (
    BadInput,
    ComputeParcel,
    DataParcel,
    DelayInput,
    InputStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    ComputeParcels,
    ComputeTransformations,
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
from diracx.logic.transformations import packing
from diracx.logic.transformations.packers import (
    FixedSizePackerArgs,
    fixed_size_packer,
)
from diracx.logic.transformations.packing import (
    content_hash,
    load_packer,
    pack_transformation,
)

PROCESS_ID = b"\x0a" * 32
TEMPLATE = {"cpu": 1, "platform": "x86_64"}


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
    workgraph_id, ownership_id, transformation_id = uuid7(), uuid7(), uuid7()
    template_id = content_hash(TEMPLATE)
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
                kind=TransformationKind.COMPUTE,
                status=TransformationStatus.ACTIVE,
                feeder="SomeFeeder",
                packer="Test",
                packer_args={"group_size": 2},
            )
        )
        await db.conn.execute(
            insert(Processes).values(process_id=PROCESS_ID, process={"class": "Tool"})
        )
        await db.conn.execute(
            insert(RequirementsTemplates).values(
                requirements_template_id=template_id, requirements_template=TEMPLATE
            )
        )
        await db.conn.execute(
            insert(ComputeTransformations).values(
                transformation_id=transformation_id,
                process_id=PROCESS_ID,
                requirements_template_id=template_id,
            )
        )
    return transformation_id


@pytest.fixture
def use_packer(monkeypatch):
    def use(plugin, args_model=FixedSizePackerArgs):
        monkeypatch.setattr(packing, "load_packer", lambda name: (plugin, args_model))

    use(fixed_size_packer)
    return use


async def add_inputs(transformation_db, transformation_id, n: int) -> list[UUID]:
    input_ids = [uuid7() for _ in range(n)]
    async with transformation_db as db:
        await db.conn.execute(
            insert(Inputs),
            [
                {
                    "TransformationID": transformation_id,
                    "InputID": input_id,
                    "InputHash": input_id.bytes,
                    "LFN": f"/lhcb/{input_id}",
                }
                for input_id in input_ids
            ],
        )
    return input_ids


async def pack(transformation_db, transformation_id, *, flush=False, max_inputs=100):
    async with transformation_db as db:
        return await pack_transformation(
            db, transformation_id, flush=flush, max_inputs=max_inputs
        )


async def input_statuses(transformation_db) -> dict[UUID, InputStatus]:
    async with transformation_db as db:
        return dict(
            (await db.conn.execute(select(Inputs.input_id, Inputs.status))).all()
        )


async def parcel_inputs(transformation_db) -> list[set[UUID]]:
    async with transformation_db as db:
        rows = (
            await db.conn.execute(
                select(ParcelInputs.parcel_id, ParcelInputs.input_id).order_by(
                    ParcelInputs.parcel_id
                )
            )
        ).all()
    groups: dict[UUID, set[UUID]] = {}
    for parcel_id, input_id in rows:
        groups.setdefault(parcel_id, set()).add(input_id)
    return list(groups.values())


async def set_transformation(transformation_db, **values):
    async with transformation_db as db:
        await db.conn.execute(update(Transformations).values(**values))


async def test_fixed_size_holds_remainder_while_feeding(
    transformation_db, transformation_id, use_packer
):
    input_ids = await add_inputs(transformation_db, transformation_id, 5)

    summary = await pack(transformation_db, transformation_id)

    assert summary.model_dump() == {
        "skipped": False,
        "parcels_created": 2,
        "inputs_assigned": 4,
        "inputs_problematic": 0,
        "inputs_delayed": 0,
        "inputs_left": 1,
    }
    assert await parcel_inputs(transformation_db) == [
        set(input_ids[0:2]),
        set(input_ids[2:4]),
    ]
    statuses = await input_statuses(transformation_db)
    assert statuses[input_ids[4]] == InputStatus.UNASSIGNED

    # Running again packs nothing new while the feeder is still active
    summary = await pack(transformation_db, transformation_id)
    assert (summary.parcels_created, summary.inputs_left) == (0, 1)


@pytest.mark.parametrize("drain", ["flush", "feeder_disabled", "no_feeder"])
async def test_fixed_size_packs_remainder_when_draining(
    transformation_db, transformation_id, use_packer, drain
):
    input_ids = await add_inputs(transformation_db, transformation_id, 3)
    if drain == "feeder_disabled":
        await set_transformation(transformation_db, feeder_enabled=False)
    elif drain == "no_feeder":
        await set_transformation(transformation_db, feeder=None)

    summary = await pack(transformation_db, transformation_id, flush=drain == "flush")

    assert (summary.parcels_created, summary.inputs_left) == (2, 0)
    assert await parcel_inputs(transformation_db) == [
        set(input_ids[0:2]),
        set(input_ids[2:]),
    ]


async def test_max_inputs_bounds_the_claim(
    transformation_db, transformation_id, use_packer
):
    await add_inputs(transformation_db, transformation_id, 5)
    summary = await pack(transformation_db, transformation_id, max_inputs=3)
    assert (summary.parcels_created, summary.inputs_left) == (1, 1)


async def test_paging_through_many_inputs(
    transformation_db, transformation_id, use_packer, monkeypatch
):
    monkeypatch.setattr(packing, "CLAIM_BATCH_SIZE", 2)
    monkeypatch.setattr(packing, "PARCEL_BATCH_SIZE", 2)
    input_ids = await add_inputs(transformation_db, transformation_id, 7)
    await set_transformation(transformation_db, feeder_enabled=False)

    summary = await pack(transformation_db, transformation_id)

    assert summary.parcels_created == 4
    assert summary.inputs_assigned == 7
    statuses = await input_statuses(transformation_db)
    assert set(statuses.values()) == {InputStatus.ASSIGNED}
    assert len(statuses) == len(input_ids)


async def test_requirements_merged_over_template(
    transformation_db, transformation_id, use_packer
):
    await add_inputs(transformation_db, transformation_id, 3)

    async def plugin(*, packer_args, inputs, **_):
        group = [i.input_id async for i in inputs]
        yield ComputeParcel(
            inputs=group[:1],
            requirements={"cpu": 8},
            parameters={"seed": 1},
            metadata={"note": "big"},
        )
        yield ComputeParcel(inputs=group[1:2], requirements={"cpu": 8})
        yield ComputeParcel(inputs=group[2:])

    use_packer(plugin)
    await pack(transformation_db, transformation_id)

    async with transformation_db as db:
        rows = (
            await db.conn.execute(
                select(
                    RequirementsTemplates.requirements_template,
                    ComputeParcels.process_id,
                    ComputeParcels.parameters,
                    Parcels.annotations,
                )
                .join(RequirementsTemplates)
                .join(Parcels)
                .order_by(Parcels.parcel_id)
            )
        ).all()
        n_requirements = len(
            (await db.conn.execute(select(RequirementsTemplates))).all()
        )
    merged = {"cpu": 8, "platform": "x86_64"}
    assert rows == [
        (merged, PROCESS_ID, {"seed": 1}, {"note": "big"}),
        (merged, PROCESS_ID, None, None),
        (TEMPLATE, PROCESS_ID, None, None),
    ]
    # The template and one merged set, stored once each
    assert n_requirements == 2


async def test_bad_and_delayed_inputs(transformation_db, transformation_id, use_packer):
    input_ids = await add_inputs(transformation_db, transformation_id, 3)
    later = datetime.now(tz=timezone.utc) + timedelta(hours=1)

    async def plugin(*, packer_args, inputs, **_):
        offered = [i.input_id async for i in inputs]
        yield BadInput(input_id=offered[0], reason="corrupt")
        yield DelayInput(input_ids=offered[1:], delay_until=later)

    use_packer(plugin)
    summary = await pack(transformation_db, transformation_id)

    assert (summary.inputs_problematic, summary.inputs_delayed) == (1, 2)
    assert summary.inputs_left == 0
    statuses = await input_statuses(transformation_db)
    assert statuses[input_ids[0]] == InputStatus.PROBLEMATIC
    async with transformation_db as db:
        data = await db.conn.scalar(select(TransformationLog.data))
    assert data == {"InputID": str(input_ids[0]), "Reason": "corrupt"}

    # Delayed inputs are not offered again before their time
    use_packer(fixed_size_packer)
    summary = await pack(transformation_db, transformation_id, flush=True)
    assert summary.parcels_created == 0


async def test_sync_packer(transformation_db, transformation_id, use_packer):
    await add_inputs(transformation_db, transformation_id, 2)

    def plugin(*, packer_args, inputs, **_):
        yield ComputeParcel(inputs=[i.input_id for i in inputs])

    use_packer(plugin)
    summary = await pack(transformation_db, transformation_id)
    assert summary.inputs_assigned == 2


@pytest.mark.parametrize("mistake", ["unoffered", "twice"])
async def test_invalid_proposals_roll_back(
    transformation_db, transformation_id, use_packer, mistake
):
    await add_inputs(transformation_db, transformation_id, 2)

    async def plugin(*, packer_args, inputs, **_):
        offered = [i.input_id async for i in inputs]
        yield ComputeParcel(inputs=offered[:1])
        if mistake == "unoffered":
            yield ComputeParcel(inputs=[uuid4()])
        else:
            yield BadInput(input_id=offered[0])

    use_packer(plugin)
    with pytest.raises(ValueError, match=mistake.replace("unoffered", "not offered")):
        await pack(transformation_db, transformation_id)
    statuses = await input_statuses(transformation_db)
    assert set(statuses.values()) == {InputStatus.UNASSIGNED}


async def test_inactive_transformation_is_skipped(
    transformation_db, transformation_id, use_packer
):
    await add_inputs(transformation_db, transformation_id, 2)
    await set_transformation(transformation_db, status=TransformationStatus.PAUSED)
    summary = await pack(transformation_db, transformation_id, flush=True)
    assert summary == packing.PackingSummary(skipped=True)


async def test_invalid_packer_args(transformation_db, transformation_id, use_packer):
    await set_transformation(transformation_db, packer_args={"group_size": 0})
    with pytest.raises(ConfigurationError, match="Invalid arguments"):
        await pack(transformation_db, transformation_id)


async def test_data_is_not_supported(transformation_db, transformation_id, use_packer):
    await add_inputs(transformation_db, transformation_id, 1)

    async def plugin(*, packer_args, inputs, **_):
        offered = [i.input_id async for i in inputs]
        yield DataParcel(inputs=offered, request={})

    use_packer(plugin)
    with pytest.raises(NotImplementedError):
        await pack(transformation_db, transformation_id)

    await set_transformation(transformation_db, kind=TransformationKind.DATA)
    with pytest.raises(NotImplementedError):
        await pack(transformation_db, transformation_id)


def test_load_packer_finds_fixed_size():
    assert load_packer("FixedSize") == (fixed_size_packer, FixedSizePackerArgs)


def test_load_packer_unknown():
    with pytest.raises(ConfigurationError, match="No packer named"):
        load_packer("DoesNotExist")


def test_load_packer_needs_an_args_model(monkeypatch):
    class EntryPoint:
        def load(self):
            def plugin(*, packer_args: dict, inputs): ...

            return plugin

    monkeypatch.setattr(packing, "select_from_extension", lambda **_: [EntryPoint()])
    with pytest.raises(ConfigurationError, match="pydantic model"):
        load_packer("Untyped")


def test_content_hash_is_canonical():
    assert content_hash({"a": 1, "b": [1, 2]}) == content_hash({"b": [1, 2], "a": 1})
    assert len(content_hash({})) == 32
    assert content_hash({"a": 1}) != content_hash({"a": 2})
