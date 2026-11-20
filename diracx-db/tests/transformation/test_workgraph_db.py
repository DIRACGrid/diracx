"""Tests for the workgraph methods of TransformationDB."""

from __future__ import annotations

import pytest
from sqlalchemy import event, func, insert, select
from sqlalchemy.exc import IntegrityError
from uuid_utils import uuid7

from diracx.core.exceptions import WorkgraphNotFoundError
from diracx.core.hashing import hash_json
from diracx.core.models.transformation import (
    TransformationKind,
    TransformationStatus,
    WorkgraphStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.db import NewTransformation
from diracx.db.sql.transformation.schema import (
    ComputeTransformations,
    Ownership,
    Processes,
    Transformations,
    Workgraphs,
)

PROCESS = {"cwlVersion": "v1.2", "class": "CommandLineTool", "baseCommand": "echo"}
PROCESS_ID = hash_json(PROCESS)


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


def new_transformation(**overrides) -> NewTransformation:
    values = {
        "transformation_id": uuid7(),
        "type": "MCSimulation",
        "packer": "FixedSize",
        "packer_args": {"group_size": 2},
        "feeder": "Seeds",
        "feeder_args": {"target_events": 1000},
        "annotations": {"name": "MCSimulation"},
        "process_id": PROCESS_ID,
        "requirements_template_id": None,
    }
    return NewTransformation(**{**values, **overrides})


async def insert_workgraph(db: TransformationDB, *transformations) -> object:
    workgraph_id = uuid7()
    await db.insert_processes({PROCESS_ID: PROCESS})
    await db.insert_workgraph(
        workgraph_id=workgraph_id,
        vo="lhcb",
        sub="lhcb:someone",
        type="MCSimulation",
        stripped_spec={"class": "Workflow"},
        annotations={"label": "test"},
        transformations=list(transformations),
    )
    return workgraph_id


async def count(db: TransformationDB, table) -> int:
    return (await db.conn.execute(select(func.count()).select_from(table))).scalar_one()


async def test_insert_processes_is_idempotent(transformation_db):
    async with transformation_db as db:
        await db.insert_processes({PROCESS_ID: PROCESS})
        await db.insert_processes({PROCESS_ID: PROCESS})
        await db.insert_processes({})
        assert await count(db, Processes) == 1


async def test_insert_workgraph(transformation_db):
    transformation = new_transformation()
    async with transformation_db as db:
        workgraph_id = await insert_workgraph(db, transformation)

    async with transformation_db as db:
        workgraph = (await db.conn.execute(select(Workgraphs))).one()
        assert workgraph.WorkgraphID == workgraph_id
        assert workgraph.Status == WorkgraphStatus.NEW
        assert workgraph.StrippedSpec == {"class": "Workflow"}

        ownership = (await db.conn.execute(select(Ownership))).one()
        assert (ownership.VO, ownership.Sub, ownership.WorkgraphID) == (
            "lhcb",
            "lhcb:someone",
            workgraph_id,
        )

        row = (await db.conn.execute(select(Transformations))).one()
        assert row.OwnershipID == ownership.OwnershipID
        assert row.Kind == TransformationKind.COMPUTE
        assert row.Status == TransformationStatus.NEW
        assert (row.Feeder, row.FeederArgs) == ("Seeds", {"target_events": 1000})
        assert (row.Packer, row.PackerArgs) == ("FixedSize", {"group_size": 2})

        # The rows the packer reads back
        assert await db.get_compute_facet(transformation.transformation_id) == (
            PROCESS_ID,
            {},
        )
        details = await db.get_transformation(transformation.transformation_id)
        assert details["feeder"] == "Seeds"
        assert details["feeder_enabled"] is True


async def test_insert_workgraph_with_requirements(transformation_db):
    requirements = {"priority": 2}
    requirements_id = hash_json(requirements)
    transformation = new_transformation(requirements_template_id=requirements_id)
    async with transformation_db as db:
        await db.insert_requirements(requirements_id, requirements)
        await insert_workgraph(db, transformation)
    async with transformation_db as db:
        assert await db.get_compute_facet(transformation.transformation_id) == (
            PROCESS_ID,
            requirements,
        )


async def test_insert_workgraph_needs_stored_process(transformation_db):
    with pytest.raises(IntegrityError):
        async with transformation_db as db:
            await db.insert_workgraph(
                workgraph_id=uuid7(),
                vo="lhcb",
                sub="s",
                type="MCSimulation",
                stripped_spec={},
                annotations=None,
                transformations=[new_transformation()],
            )
    async with transformation_db as db:
        assert await count(db, Workgraphs) == 0
        assert await count(db, ComputeTransformations) == 0


async def test_get_workgraph(transformation_db):
    async with transformation_db as db:
        workgraph_id = await insert_workgraph(db, new_transformation())
        assert await db.get_workgraph(workgraph_id) == {
            "status": WorkgraphStatus.NEW,
            "type": "MCSimulation",
            "vo": "lhcb",
            "sub": "lhcb:someone",
        }
        with pytest.raises(WorkgraphNotFoundError):
            await db.get_workgraph(uuid7())


async def test_set_workgraph_status(transformation_db):
    async with transformation_db as db:
        workgraph_id = await insert_workgraph(db, new_transformation())
        assert await db.set_workgraph_status(
            workgraph_id, WorkgraphStatus.NEW, WorkgraphStatus.ACTIVE
        )
        # Compare-and-set: it is no longer New
        assert not await db.set_workgraph_status(
            workgraph_id, WorkgraphStatus.NEW, WorkgraphStatus.ACTIVE
        )
        assert (await db.get_workgraph(workgraph_id))[
            "status"
        ] == WorkgraphStatus.ACTIVE


async def test_set_workgraph_transformations_status(transformation_db):
    first, second = new_transformation(), new_transformation()
    async with transformation_db as db:
        workgraph_id = await insert_workgraph(db, first)
        other_workgraph_id = await insert_workgraph(db, second)

        moved = await db.set_workgraph_transformations_status(
            workgraph_id, TransformationStatus.NEW, TransformationStatus.ACTIVE
        )
        assert moved == 1
        # Only the members of that workgraph move
        assert await db.list_transformation_ids(TransformationStatus.ACTIVE) == [
            first.transformation_id
        ]
        assert await db.list_feedable_transformation_ids() == [first.transformation_id]
        assert (
            await db.set_workgraph_transformations_status(
                workgraph_id, TransformationStatus.NEW, TransformationStatus.ACTIVE
            )
            == 0
        )
        assert other_workgraph_id != workgraph_id


async def test_ownership_unique_per_workgraph(transformation_db):
    async with transformation_db as db:
        workgraph_id = await insert_workgraph(db, new_transformation())
    with pytest.raises(IntegrityError):
        async with transformation_db as db:
            await db.conn.execute(
                insert(Ownership).values(
                    ownership_id=uuid7(), vo="lhcb", sub="s", workgraph_id=workgraph_id
                )
            )
