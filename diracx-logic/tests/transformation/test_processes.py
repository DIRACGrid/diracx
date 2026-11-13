"""The MCSimulation workgraph of #1080, ingested end to end.

Three transformations, each a Workflow running the same tool; boole-moore
runs it through a nested workflow. The fixture is the document a client
submits, as ``cwltool --pack`` writes it.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from sqlalchemy import func, select, update

from diracx.core.cwl import (
    NotSelfContainedError,
    PolicyViolationError,
    entry_identity,
    subgraph,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import Processes
from diracx.logic.transformation import processes
from diracx.logic.transformation.processes import (
    ProcessNotFoundError,
    StoredProcess,
    load_process,
    store_process,
)
from diracx.logic.transformation.validation import InvalidProcessError

FIXTURE = (
    Path(__file__).parents[3]
    / "diracx-core"
    / "tests"
    / "fixtures"
    / "mc-simulation.packed.json"
)
TRANSFORMATIONS = ("#gauss.cwl", "#boole-moore.cwl", "#merge.cwl")


@pytest.fixture
def packed() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        yield db


def entry(document: dict, entry_id: str) -> dict:
    return next(e for e in document["$graph"] if e["id"] == entry_id)


async def count_processes(db: TransformationDB) -> int:
    return await db.conn.scalar(select(func.count()).select_from(Processes))


async def test_each_transformation_is_one_row_a_runner_can_start(
    transformation_db: TransformationDB, packed
):
    async with transformation_db as db:
        ids = {e: await store_process(db, packed, e) for e in TRANSFORMATIONS}
        assert await count_processes(db) == 3
        loaded = {e: await load_process(db, pid) for e, pid in ids.items()}
    for entry_id, process_id in ids.items():
        assert process_id == entry_identity(packed, entry_id)
        assert loaded[entry_id] == StoredProcess(subgraph(packed, entry_id), entry_id)


async def test_resubmitting_costs_a_lookup_and_no_validation(
    transformation_db: TransformationDB, packed, monkeypatch
):
    async with transformation_db as db:
        first = await store_process(db, packed, "#merge.cwl")

        def refuse(_):
            raise AssertionError("validated a stored process")

        monkeypatch.setattr(processes, "validate_packed", refuse)
        again = await store_process(db, copy.deepcopy(packed), "#merge.cwl")
        assert first == again
        assert await count_processes(db) == 1


async def test_an_edit_stores_a_new_row_and_keeps_the_old(
    transformation_db: TransformationDB, packed
):
    async with transformation_db as db:
        before = await store_process(db, packed, "#merge.cwl")
        entry(packed, "#merge.cwl")["doc"] = "Merge differently"
        after = await store_process(db, packed, "#merge.cwl")
        assert before != after
        assert await count_processes(db) == 2


@pytest.mark.parametrize(
    "mutate, error",
    [
        (
            lambda d: entry(d, "#merge.cwl")["steps"][0].update(run="merge.cwl"),
            NotSelfContainedError,
        ),
        (
            lambda d: entry(d, "#lb-run-app.cwl")["requirements"].append(
                {"class": "DockerRequirement", "dockerFile": "FROM alpine"}
            ),
            PolicyViolationError,
        ),
        (
            lambda d: entry(d, "#merge.cwl").update(nonsense=1),
            InvalidProcessError,
        ),
    ],
)
async def test_refused_processes_store_nothing(
    transformation_db: TransformationDB, packed, mutate, error
):
    mutate(packed)
    async with transformation_db as db:
        with pytest.raises(error):
            await store_process(db, packed, "#merge.cwl")
        assert await count_processes(db) == 0


async def test_an_unknown_entry_is_refused(transformation_db: TransformationDB, packed):
    async with transformation_db as db:
        with pytest.raises(KeyError):
            await store_process(db, packed, "#nowhere.cwl")


async def test_loading_an_unknown_process_is_refused(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        with pytest.raises(ProcessNotFoundError):
            await load_process(db, b"x" * 32)


async def test_a_stored_row_that_does_not_match_its_identity_is_refused(
    transformation_db: TransformationDB, packed
):
    async with transformation_db as db:
        merge = await store_process(db, packed, "#merge.cwl")
        gauss = await store_process(db, packed, "#gauss.cwl")
        swapped = (await db.get_processes([gauss]))[gauss]
        await db.conn.execute(
            update(Processes)
            .where(Processes.process_id == merge)
            .values({Processes.process: swapped})
        )
        with pytest.raises(ValueError, match="does not match its content"):
            await load_process(db, merge)
