"""Tests for single-transformation workgraph submission (DX-ADR-007)."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import func, select, update

from diracx.core.exceptions import (
    InvalidWorkgraphError,
    InvalidWorkgraphTransitionError,
    WorkgraphNotFoundError,
)
from diracx.core.hashing import hash_json
from diracx.core.models.transformation import TransformationStatus, WorkgraphStatus
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    Processes,
    RequirementsTemplates,
    TransformationLog,
    Transformations,
    WorkgraphLog,
    Workgraphs,
)
from diracx.logic.transformations import (
    compile_workgraph,
    start_workgraph,
    submit_workgraph,
)
from diracx.logic.transformations.workgraphs import PROCESS_URI_PREFIX

EXAMPLES = Path(__file__).parent / "workgraphs"


def load(name: str) -> dict[str, Any]:
    return yaml.safe_load((EXAMPLES / name).read_text())


# --------------------------------------------------------------------------
# Compilation
# --------------------------------------------------------------------------


def test_simulation():
    document = load("simulation.cwl")
    original = copy.deepcopy(document)
    compiled = compile_workgraph(document)
    assert document == original

    t = compiled.transformation
    assert compiled.type == t.type == "MCSimulation"
    assert t.feeder == "Seeds"
    assert t.feeder_args == {"target_events": 600000, "events_per_seed": 1000}
    assert (t.packer, t.packer_args) == ("FixedSize", {"group_size": 1})
    assert t.requirements_template_id is None
    assert compiled.requirements_templates == {}
    assert t.annotations == {
        "name": "MCSimulation",
        "label": "MCSimulation",
        "doc": "Gauss, one seed per job",
    }
    assert compiled.annotations == {
        "id": "MC_2026_Bs2JpsiPhi_Sim12",
        "label": "MC simulation",
        "doc": "Gauss, one seed per job.",
    }

    # The run body is stored under its hash and replaced by it in the spec
    run = document["steps"]["MCSimulation"]["run"]
    assert t.process_id == hash_json(run)
    assert compiled.processes == {t.process_id: run}
    stripped_step = compiled.stripped_spec["steps"]["MCSimulation"]
    assert stripped_step["run"] == f"{PROCESS_URI_PREFIX}{t.process_id.hex()}"
    assert stripped_step["in"] == {"seed": "events"}


def test_data_processing_list_forms():
    """List-form hints, inputs and steps; a non-fed input is a parameter."""
    compiled = compile_workgraph(load("data-processing.cwl"))
    t = compiled.transformation
    assert compiled.type == "Sprucing"
    assert t.feeder == "LHCbBookkeeping"
    assert t.feeder_args["fileType"] == "RAW"
    requirements = {"priority": 2, "output_se": "CERN-BUFFER"}
    assert t.requirements_template_id == hash_json(requirements)
    assert compiled.requirements_templates == {hash_json(requirements): requirements}
    assert compiled.stripped_spec["steps"][0]["run"].startswith(PROCESS_URI_PREFIX)
    assert t.annotations == {"name": "sprucing"}


def test_without_feeder():
    t = compile_workgraph(load("parameters-only.cwl")).transformation
    assert (t.feeder, t.feeder_args) == (None, None)


def test_identifiers_are_minted_per_compilation():
    first = compile_workgraph(load("simulation.cwl"))
    second = compile_workgraph(load("simulation.cwl"))
    assert first.workgraph_id != second.workgraph_id
    assert (
        first.transformation.transformation_id
        != second.transformation.transformation_id
    )
    assert first.transformation.process_id == second.transformation.process_id


# --------------------------------------------------------------------------
# Invalid documents
# --------------------------------------------------------------------------


def errors_of(document: Any) -> list[str]:
    with pytest.raises(InvalidWorkgraphError) as exc_info:
        compile_workgraph(document)
    return exc_info.value.errors


def simulation() -> dict[str, Any]:
    return load("simulation.cwl")


def test_not_an_object():
    assert errors_of([]) == ["the document must be a JSON object"]


def test_wrong_cwl_version():
    document = simulation()
    document["cwlVersion"] = "v1.0"
    assert errors_of(document) == ["cwlVersion must be v1.2, not 'v1.0'"]


def test_not_a_workflow():
    document = simulation()
    document["class"] = "CommandLineTool"
    assert errors_of(document) == [
        "a workgraph must be a Workflow, not 'CommandLineTool'"
    ]


def test_missing_workgraph_hint():
    document = simulation()
    del document["hints"]
    assert errors_of(document) == ["the workflow must carry the dirac:Workgraph hint"]


def test_missing_type():
    document = simulation()
    del document["hints"]["dirac:Workgraph"]["type"]
    assert errors_of(document) == ["dirac:Workgraph.type: Field required"]


def test_unsupported_hint_field():
    """Fields beyond deliverable 1 are rejected, not silently dropped."""
    document = simulation()
    document["hints"]["dirac:Workgraph"]["approving"] = [{"action": "Manual"}]
    assert errors_of(document) == [
        "dirac:Workgraph.approving: Extra inputs are not permitted"
    ]


def test_several_steps():
    document = simulation()
    step = document["steps"]["MCSimulation"]
    document["steps"]["MCReconstruction"] = copy.deepcopy(step)
    assert errors_of(document) == [
        "a workgraph must have exactly one step for now, not 2"
    ]


def test_no_step():
    document = simulation()
    document["steps"] = {}
    assert errors_of(document) == [
        "a workgraph must have exactly one step for now, not 0"
    ]


def test_bare_step():
    document = simulation()
    del document["steps"]["MCSimulation"]["hints"]
    assert errors_of(document) == [
        "steps.MCSimulation: the step must carry dirac:Transformation"
    ]


def test_missing_packer():
    document = simulation()
    del document["steps"]["MCSimulation"]["hints"]["dirac:Transformation"]["packer"]
    assert errors_of(document) == [
        "steps.MCSimulation.dirac:Transformation.packer: Field required"
    ]


def test_run_reference():
    document = simulation()
    document["steps"]["MCSimulation"]["run"] = "transformations/gauss.cwl"
    (error,) = errors_of(document)
    assert error.startswith("steps.MCSimulation.run: must be an inline process")


def test_unknown_source():
    document = simulation()
    document["steps"]["MCSimulation"]["in"] = {"seed": "nothing"}
    assert errors_of(document) == [
        "steps.MCSimulation.in.seed: source nothing is not a workflow input"
    ]


def test_several_fed_inputs():
    document = simulation()
    document["inputs"]["more-events"] = copy.deepcopy(document["inputs"]["events"])
    document["steps"]["MCSimulation"]["in"]["extra"] = "more-events"
    (error,) = errors_of(document)
    assert "are all fed" in error


def test_invalid_feeder():
    document = simulation()
    document["inputs"]["events"]["dirac:Feeder"] = {"args": {}}
    assert errors_of(document) == ["inputs.events.dirac:Feeder.name: Field required"]


def data_processing() -> dict[str, Any]:
    return load("data-processing.cwl")


def test_repeated_hint_class():
    """A hint class appears at most once (DX-ADR-007), the first is not overridden."""
    document = data_processing()
    document["hints"].append(
        {"class": "dirac:Workgraph", "schema_version": "1.0", "type": "Other"}
    )
    assert errors_of(document) == [
        "workflow: hint dirac:Workgraph appears more than once"
    ]


def test_non_string_hint_class():
    document = data_processing()
    document["hints"].append({"class": ["dirac:Workgraph"]})
    assert errors_of(document) == [
        "workflow: a hint class must be a string, not ['dirac:Workgraph']"
    ]


def test_repeated_input_id():
    document = data_processing()
    document["inputs"].append({"id": "raw-data", "type": "string"})
    assert errors_of(document) == ["inputs: id raw-data appears more than once"]


def test_non_string_id():
    document = data_processing()
    document["inputs"].append({"id": 7, "type": "string"})
    assert errors_of(document) == ["inputs: an id must be a string, not 7"]


def test_repeated_step_input_id():
    document = data_processing()
    step_inputs = document["steps"][0]["in"]
    step_inputs.append(copy.deepcopy(step_inputs[0]))
    assert errors_of(document) == [
        "steps.sprucing.in: id input-data appears more than once"
    ]


def test_errors_are_collected():
    document = simulation()
    document["cwlVersion"] = "v1.0"
    del document["steps"]["MCSimulation"]["hints"]
    assert len(errors_of(document)) == 2


# --------------------------------------------------------------------------
# Submission and start
# --------------------------------------------------------------------------


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
            await db.post_create(conn)
        yield db


async def count(db: TransformationDB, table) -> int:
    return (await db.conn.execute(select(func.count()).select_from(table))).scalar_one()


async def submit(db: TransformationDB, name: str):
    return await submit_workgraph(
        load(name), transformation_db=db, vo="lhcb", sub="lhcb:someone"
    )


async def test_submit_workgraph(transformation_db):
    async with transformation_db as db:
        result = await submit(db, "data-processing.cwl")

    async with transformation_db as db:
        workgraph = await db.get_workgraph(result.workgraph_id)
        assert workgraph["status"] == WorkgraphStatus.NEW
        assert (workgraph["vo"], workgraph["sub"]) == ("lhcb", "lhcb:someone")
        transformation = await db.get_transformation(result.transformation_id)
        assert transformation["status"] == TransformationStatus.NEW
        assert transformation["feeder"] == "LHCbBookkeeping"
        process_id, requirements = await db.get_compute_facet(result.transformation_id)
        assert requirements == {"priority": 2, "output_se": "CERN-BUFFER"}
        assert await count(db, Processes) == 1
        assert await count(db, RequirementsTemplates) == 1


async def test_resubmission_reuses_the_process(transformation_db):
    async with transformation_db as db:
        first = await submit(db, "simulation.cwl")
        second = await submit(db, "simulation.cwl")
    assert first.workgraph_id != second.workgraph_id
    async with transformation_db as db:
        assert await count(db, Workgraphs) == 2
        assert await count(db, Transformations) == 2
        assert await count(db, Processes) == 1


async def test_invalid_submission_stores_nothing(transformation_db):
    document = simulation()
    del document["steps"]["MCSimulation"]["hints"]
    async with transformation_db as db:
        with pytest.raises(InvalidWorkgraphError):
            await submit_workgraph(document, transformation_db=db, vo="lhcb", sub="s")
        assert await count(db, Workgraphs) == 0
        assert await count(db, Processes) == 0


async def test_start_workgraph(transformation_db):
    async with transformation_db as db:
        result = await submit(db, "simulation.cwl")
        # Not feedable until started
        assert await db.list_feedable_transformation_ids() == []

    async with transformation_db as db:
        await start_workgraph(
            result.workgraph_id, transformation_db=db, author="someone"
        )

    async with transformation_db as db:
        workgraph = await db.get_workgraph(result.workgraph_id)
        assert workgraph["status"] == WorkgraphStatus.ACTIVE
        transformation = await db.get_transformation(result.transformation_id)
        assert transformation["status"] == TransformationStatus.ACTIVE
        assert [str(t) for t in await db.list_feedable_transformation_ids()] == [
            str(result.transformation_id)
        ]
        # Each move is in the audit trail
        assert [
            (row.OldStatus, row.NewStatus, row.Author)
            for row in (await db.conn.execute(select(WorkgraphLog))).all()
        ] == [(WorkgraphStatus.NEW, WorkgraphStatus.ACTIVE, "someone")]
        assert [
            (row.OldStatus, row.NewStatus, row.Author)
            for row in (await db.conn.execute(select(TransformationLog))).all()
        ] == [(TransformationStatus.NEW, TransformationStatus.ACTIVE, "someone")]


async def test_start_twice(transformation_db):
    async with transformation_db as db:
        result = await submit(db, "simulation.cwl")
        await start_workgraph(
            result.workgraph_id, transformation_db=db, author="someone"
        )
        with pytest.raises(InvalidWorkgraphTransitionError) as exc_info:
            await start_workgraph(
                result.workgraph_id, transformation_db=db, author="someone"
            )
    assert (exc_info.value.from_status, exc_info.value.to_status) == (
        "Active",
        "Active",
    )


async def test_start_unknown_workgraph(transformation_db):
    from uuid_utils import uuid7

    async with transformation_db as db:
        with pytest.raises(WorkgraphNotFoundError):
            await start_workgraph(uuid7(), transformation_db=db, author="someone")


async def test_start_without_new_transformations(transformation_db):
    """A New workgraph whose members are not New does not start at all."""
    async with transformation_db as db:
        result = await submit(db, "simulation.cwl")
        # Break the invariant that a New workgraph has only New members
        await db.conn.execute(
            update(Transformations)
            .where(Transformations.transformation_id == result.transformation_id)
            .values(status=TransformationStatus.PAUSED)
        )

    with pytest.raises(InvalidWorkgraphTransitionError) as exc_info:
        async with transformation_db as db:
            await start_workgraph(
                result.workgraph_id, transformation_db=db, author="someone"
            )
    assert "none of its transformations is New" in str(exc_info.value)

    # The workgraph's own move was rolled back with its log row
    async with transformation_db as db:
        workgraph = await db.get_workgraph(result.workgraph_id)
        assert workgraph["status"] == WorkgraphStatus.NEW
        assert await count(db, WorkgraphLog) == 0
