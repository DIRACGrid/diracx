"""The MCSimulation workgraph of #1080, ingested end to end.

The workgraph has three transformations, each a Workflow around the same
shared tool; boole-moore runs a nested sub-workflow (the "6-process" case).
The fixture is the JSON a client submits: the YAML files it was resolved from,
and the tests of resolving them, are in diracx-cli.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

from diracx.core.cwl import decompose, unpack
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import Processes
from diracx.logic.transformation.processes import load_process, store_process
from diracx.logic.transformation.validation import validate_packed

FIXTURE = Path(__file__).parent / "fixtures" / "mc-simulation.json"


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        yield db


def transformations() -> dict[str, dict]:
    workgraph = json.loads(FIXTURE.read_text())
    return {name: step["run"] for name, step in workgraph["steps"].items()}


async def test_each_transformation_is_one_valid_row(
    transformation_db: TransformationDB,
):
    runs = transformations()
    async with transformation_db as db:
        ids = {name: await store_process(db, run) for name, run in runs.items()}
        rows = await db.conn.scalar(select(func.count()).select_from(Processes))
        payloads = {name: await load_process(db, pid) for name, pid in ids.items()}
    assert set(ids) == {"MCSimulation", "MCReconstruction", "MCMerge"}
    assert rows == 3
    for name, payload in payloads.items():
        validate_packed(payload)
        assert unpack(payload) == decompose(runs[name])


async def test_tree_shape(transformation_db: TransformationDB):
    runs = transformations()
    nodes = {name: decompose(run)[1] for name, run in runs.items()}
    # wrapper + shared tool; wrapper + sub-workflow + shared tool
    assert len(nodes["MCSimulation"]) == 2
    assert len(nodes["MCReconstruction"]) == 3
    shared = set.intersection(*(set(n) for n in nodes.values()))
    assert len(shared) == 1, "lb-run-app.cwl is one process in all three"


async def test_resubmitting_is_a_no_op(transformation_db: TransformationDB):
    async with transformation_db as db:
        first = [await store_process(db, run) for run in transformations().values()]
        again = [await store_process(db, run) for run in transformations().values()]
        rows = await db.conn.scalar(select(func.count()).select_from(Processes))
    assert first == again
    assert rows == 3
