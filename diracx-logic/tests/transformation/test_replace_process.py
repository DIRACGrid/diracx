"""An operator replaces the process of a paused transformation, for a hotfix."""

from __future__ import annotations

import pytest
from sqlalchemy import insert, select, update
from uuid_utils import uuid7

from diracx.core.cwl import PolicyViolationError
from diracx.core.models.auth import UserInfo
from diracx.core.models.transformation import ParcelStatus, TransformationStatus
from diracx.core.properties import NORMAL_USER, OPERATOR, PRODUCTION_MANAGEMENT
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    ComputeParcels,
    ComputeTransformations,
    Ownership,
    Parcels,
    TransformationLog,
    Transformations,
    Workgraphs,
)
from diracx.logic.transformation.processes import (
    PROCESS_REPLACED,
    IncompatibleProcessError,
    ProcessReplacementForbiddenError,
    TransformationNotFoundError,
    TransformationNotPausedError,
    replace_transformation_process,
    store_process,
)
from diracx.logic.transformation.validation import InvalidProcessError

OPERATOR_INFO = UserInfo(
    sub="lhcb:operator-sub",
    preferred_username="an-operator",
    dirac_group="lhcb_prmgr",
    vo="lhcb",
)


def gauss(version: str, input_type: str = "string") -> dict:
    tool = {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": ["lb-prod-run"],
        "arguments": [
            f'{{"application": {{"name": "Gauss", "version": "{version}"}}}}'
        ],
        "inputs": {"seed": {"type": input_type, "inputBinding": {"prefix": "--seed"}}},
        "outputs": {"sim": {"type": "File[]", "outputBinding": {"glob": "*.sim"}}},
    }
    return {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "inputs": {"seed": input_type},
        "outputs": {"sim-files": {"type": "File[]", "outputSource": "gauss/sim"}},
        "steps": {"gauss": {"run": tool, "in": {"seed": "seed"}, "out": ["sim"]}},
    }


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        yield db


@pytest.fixture
async def paused(transformation_db: TransformationDB):
    """A paused transformation running Gauss v57r1, with a parcel per state."""
    workgraph_id, ownership_id, transformation_id = uuid7(), uuid7(), uuid7()
    parcels = {status: uuid7() for status in ParcelStatus}
    async with transformation_db as db:
        process_id = await store_process(db, gauss("v57r1"))
        await db.conn.execute(
            insert(Workgraphs).values(
                workgraph_id=workgraph_id, type="MCSimulation", stripped_spec={}
            )
        )
        await db.conn.execute(
            insert(Ownership).values(
                ownership_id=ownership_id,
                vo="lhcb",
                sub="owner-sub",
                workgraph_id=workgraph_id,
            )
        )
        await db.conn.execute(
            insert(Transformations).values(
                transformation_id=transformation_id,
                ownership_id=ownership_id,
                type="MCSimulation",
                kind="Compute",
                status=TransformationStatus.PAUSED,
                packer="PerInput",
            )
        )
        await db.conn.execute(
            insert(ComputeTransformations).values(
                transformation_id=transformation_id, process_id=process_id
            )
        )
        for status, parcel_id in parcels.items():
            await db.conn.execute(
                insert(Parcels).values(
                    parcel_id=parcel_id,
                    transformation_id=transformation_id,
                    kind="Compute",
                    status=status,
                )
            )
            await db.conn.execute(
                insert(ComputeParcels).values(
                    parcel_id=parcel_id, process_id=process_id
                )
            )
    return transformation_id, process_id, parcels


async def replace(db, transformation_id, process, properties=(PRODUCTION_MANAGEMENT,)):
    return await replace_transformation_process(
        db,
        transformation_id,
        process,
        operator=OPERATOR_INFO,
        properties=properties,
        reason="Gauss v57r2 fixes the generator crash",
    )


async def parcel_processes(db, parcels) -> dict[ParcelStatus, bytes]:
    rows = await db.conn.execute(
        select(ComputeParcels.parcel_id, ComputeParcels.process_id)
    )
    by_id = dict(rows.all())
    return {status: by_id[parcel_id] for status, parcel_id in parcels.items()}


async def test_hotfix_repoints_the_transformation_and_undispatched_parcels(
    transformation_db: TransformationDB, paused
):
    transformation_id, old, parcels = paused
    async with transformation_db as db:
        result = await replace(db, transformation_id, gauss("v57r2"))
        current = await db.conn.scalar(select(ComputeTransformations.process_id))
        processes = await parcel_processes(db, parcels)
        log = (await db.conn.execute(select(TransformationLog))).one()
    assert result.old == old != result.new
    assert current == result.new
    assert result.repointed_parcels == 1
    assert processes[ParcelStatus.UNASSIGNED] == result.new
    for status in set(ParcelStatus) - {ParcelStatus.UNASSIGNED}:
        assert processes[status] == old, f"a {status} parcel keeps what it ran"
    assert log.EventType == PROCESS_REPLACED
    assert log.Author == "an-operator"
    assert log.Message == "Gauss v57r2 fixes the generator crash"
    assert log.Data == {
        "old": old.hex(),
        "new": result.new.hex(),
        "repointed_parcels": 1,
        "operator": "lhcb:operator-sub",
    }


async def test_operator_property_is_enough(transformation_db: TransformationDB, paused):
    transformation_id, _, _ = paused
    async with transformation_db as db:
        result = await replace(db, transformation_id, gauss("v57r2"), (OPERATOR,))
    assert result.repointed_parcels == 1


async def test_end_users_cannot_replace_a_process(
    transformation_db: TransformationDB, paused
):
    transformation_id, old, _ = paused
    async with transformation_db as db:
        with pytest.raises(ProcessReplacementForbiddenError):
            await replace(db, transformation_id, gauss("v57r2"), (NORMAL_USER,))
        assert await db.conn.scalar(select(ComputeTransformations.process_id)) == old


async def test_a_reason_is_required(transformation_db: TransformationDB, paused):
    transformation_id, _, _ = paused
    async with transformation_db as db:
        with pytest.raises(ValueError, match="reason"):
            await replace_transformation_process(
                db,
                transformation_id,
                gauss("v57r2"),
                operator=OPERATOR_INFO,
                properties=(PRODUCTION_MANAGEMENT,),
                reason="  ",
            )


@pytest.mark.parametrize(
    "status", [TransformationStatus.ACTIVE, TransformationStatus.COMPLETED]
)
async def test_only_a_paused_transformation_can_change(
    transformation_db: TransformationDB, paused, status
):
    transformation_id, _, _ = paused
    async with transformation_db as db:
        await db.conn.execute(update(Transformations).values(status=status))
        with pytest.raises(TransformationNotPausedError, match="pause it first"):
            await replace(db, transformation_id, gauss("v57r2"))


async def test_the_interface_must_not_change(
    transformation_db: TransformationDB, paused
):
    transformation_id, _, _ = paused
    async with transformation_db as db:
        with pytest.raises(IncompatibleProcessError):
            await replace(db, transformation_id, gauss("v57r2", input_type="int"))


def with_seed_record(process: dict, value_type: str) -> dict:
    process["requirements"] = {
        "SchemaDefRequirement": {
            "types": [
                {"name": "Seed", "type": "record", "fields": {"value": value_type}}
            ]
        }
    }
    process["inputs"]["options"] = ["null", "Seed"]
    return process


async def test_a_named_type_must_not_change_behind_its_name(
    transformation_db: TransformationDB, paused
):
    transformation_id, _, _ = paused
    async with transformation_db as db:
        await db.conn.execute(
            update(ComputeTransformations).values(
                process_id=await store_process(
                    db, with_seed_record(gauss("v57r1"), "int")
                )
            )
        )
        same = await replace(
            db, transformation_id, with_seed_record(gauss("v57r2"), "int")
        )
        assert same.new != same.old, "same named type, so accepted"
        with pytest.raises(IncompatibleProcessError):
            await replace(
                db, transformation_id, with_seed_record(gauss("v57r3"), "string")
            )


async def test_the_replacement_is_validated(
    transformation_db: TransformationDB, paused
):
    transformation_id, _, _ = paused
    broken = gauss("v57r2")
    broken["steps"]["gauss"]["run"]["inputs"]["seed"]["type"] = "File"
    unsupported = gauss("v57r2")
    unsupported["requirements"] = {"SoftwareRequirement": {"packages": {"gauss": {}}}}
    async with transformation_db as db:
        with pytest.raises(InvalidProcessError):
            await replace(db, transformation_id, broken)
        with pytest.raises(PolicyViolationError):
            await replace(db, transformation_id, unsupported)


async def test_unknown_transformation(transformation_db: TransformationDB):
    async with transformation_db as db:
        with pytest.raises(TransformationNotFoundError):
            await replace(db, uuid7(), gauss("v57r2"))


async def test_replacing_with_the_same_process_changes_nothing(
    transformation_db: TransformationDB, paused
):
    transformation_id, old, _ = paused
    async with transformation_db as db:
        result = await replace(db, transformation_id, gauss("v57r1"))
        logged = await db.conn.scalar(select(TransformationLog.log_id))
    assert (result.old, result.new, result.repointed_parcels) == (old, old, 0)
    assert logged is None
