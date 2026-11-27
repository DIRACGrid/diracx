"""Tests for single-transformation workgraph submission (DX-ADR-007)."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import func, select, update

from diracx.core.cwl import decompose, entry_identity, format_reference, subgraph
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
from diracx.logic.transformations.workgraphs import (
    FEEDER_HINT,
    TRANSFORMATION_HINT,
    WORKGRAPH_HINT,
)

EXAMPLES = Path(__file__).parent / "workgraphs"


def load(name: str) -> dict[str, Any]:
    return yaml.safe_load((EXAMPLES / name).read_text())


def main_of(document: dict[str, Any]) -> dict[str, Any]:
    """The #main entry of a packed document."""
    return next(e for e in document["$graph"] if e["id"] == "#main")


def step_of(document: dict[str, Any]) -> dict[str, Any]:
    return main_of(document)["steps"][0]


def hint(obj: dict[str, Any], cls: str) -> dict[str, Any]:
    return next(h for h in obj["hints"] if h["class"] == cls)


# --------------------------------------------------------------------------
# Compilation
# --------------------------------------------------------------------------


def test_simulation():
    """A packed document whose step runs another $graph entry."""
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
        "id": "#main",
        "label": "MC simulation",
        "doc": "Gauss, one seed per job.",
    }

    # The process is the document the step runs, under its #1114 identity
    assert t.process_id == entry_identity(document, "#gauss.cwl")
    assert compiled.processes == {t.process_id: subgraph(document, "#gauss.cwl")}

    # The stripped spec is #main, its step running the stored process
    stripped = compiled.stripped_spec
    assert "$graph" not in stripped
    assert stripped["id"] == "#main"
    assert stripped["cwlVersion"] == "v1.2"
    assert stripped["$namespaces"] == {"dirac": "https://diracgrid.org/cwl#"}
    assert stripped["steps"][0]["run"] == format_reference(t.process_id)
    assert stripped["steps"][0]["in"] == step_of(document)["in"]


def test_data_processing_inline_run():
    """An inline run body; a non-fed input is a job parameter."""
    document = load("data-processing.cwl")
    compiled = compile_workgraph(document)
    t = compiled.transformation
    assert compiled.type == "Sprucing"
    assert t.feeder == "LHCbBookkeeping"
    assert t.feeder_args["fileType"] == "RAW"
    requirements = {"priority": 2, "output_se": "CERN-BUFFER"}
    assert t.requirements_template_id == hash_json(requirements)
    assert compiled.requirements_templates == {hash_json(requirements): requirements}
    assert t.annotations == {"name": "sprucing"}

    # The inline body has the identity it has inside the workgraph
    assert t.process_id in decompose(document)
    (stored,) = compiled.processes.values()
    assert stored["class"] == "CommandLineTool"
    assert stored["cwlVersion"] == "v1.2"
    assert entry_identity(stored) == t.process_id
    assert compiled.stripped_spec["steps"][0]["run"] == format_reference(t.process_id)


def test_single_process_document():
    """A document without $graph, with map-form fields and no feeder."""
    document = load("parameters-only.cwl")
    compiled = compile_workgraph(document)
    t = compiled.transformation
    assert (t.feeder, t.feeder_args) == (None, None)
    assert t.process_id in decompose(document)
    assert compiled.stripped_spec["steps"]["echo"]["run"] == format_reference(
        t.process_id
    )


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
    (error,) = errors_of(document)
    assert "Only CWL v1.2 is supported" in error


def test_prefixed_dirac_names_are_refused():
    """dirac: names arrive as full IRIs; check_self_contained refuses prefixes."""
    document = simulation()
    hint(main_of(document), WORKGRAPH_HINT)["class"] = "dirac:Workgraph"
    (error,) = errors_of(document)
    assert "uses the $namespaces prefix 'dirac'" in error


def test_not_self_contained():
    document = simulation()
    step_of(document)["run"] = "transformations/gauss.cwl"
    (error,) = errors_of(document)
    assert "'transformations/gauss.cwl'" in error
    assert "is not in $graph" in error


def test_packed_without_main():
    document = simulation()
    main_of(document)["id"] = "#workgraph"
    assert errors_of(document) == ["a packed workgraph needs a #main entry"]


def test_not_a_workflow():
    document = load("parameters-only.cwl")
    document["class"] = "CommandLineTool"
    del document["steps"]
    document["outputs"] = []
    errors = errors_of(document)
    assert "a workgraph must be a Workflow, not 'CommandLineTool'" in errors


def test_missing_workgraph_hint():
    document = simulation()
    del main_of(document)["hints"]
    assert errors_of(document) == ["the workflow must carry the dirac:Workgraph hint"]


def test_missing_type():
    document = simulation()
    del hint(main_of(document), WORKGRAPH_HINT)["type"]
    assert errors_of(document) == ["dirac:Workgraph.type: Field required"]


def test_unsupported_hint_field():
    """Fields beyond deliverable 1 are rejected, not silently dropped."""
    document = simulation()
    hint(main_of(document), WORKGRAPH_HINT)["approving"] = [{"action": "Manual"}]
    assert errors_of(document) == [
        "dirac:Workgraph.approving: Extra inputs are not permitted"
    ]


def test_several_steps():
    document = simulation()
    second = copy.deepcopy(step_of(document))
    second["id"] = "#main/MCReconstruction"
    main_of(document)["steps"].append(second)
    assert errors_of(document) == [
        "a workgraph must have exactly one step for now, not 2"
    ]


def test_no_step():
    document = load("parameters-only.cwl")
    document["steps"] = {}
    assert errors_of(document) == [
        "a workgraph must have exactly one step for now, not 0"
    ]


def test_bare_step():
    document = simulation()
    del step_of(document)["hints"]
    assert errors_of(document) == [
        "steps.MCSimulation: the step must carry dirac:Transformation"
    ]


def test_missing_packer():
    document = simulation()
    del hint(step_of(document), TRANSFORMATION_HINT)["packer"]
    assert errors_of(document) == [
        "steps.MCSimulation.dirac:Transformation.packer: Field required"
    ]


def test_inline_run_of_graph_entries():
    """An inline workflow running $graph entries waits for store_process."""
    document = simulation()
    step_of(document)["run"] = {
        "class": "Workflow",
        "inputs": [],
        "outputs": [],
        "steps": [{"id": "#inner/gauss", "run": "#gauss.cwl", "in": [], "out": []}],
    }
    (error,) = errors_of(document)
    assert error.startswith("steps.MCSimulation.run: an inline process running")


def test_unknown_source():
    document = simulation()
    step_of(document)["in"][0]["source"] = "#main/nothing"
    assert errors_of(document) == [
        "steps.MCSimulation.in.seed: source nothing is not a workflow input"
    ]


def test_several_fed_inputs():
    document = simulation()
    inputs = main_of(document)["inputs"]
    extra = copy.deepcopy(inputs[0])
    extra["id"] = "#main/more-events"
    inputs.append(extra)
    step_of(document)["in"].append(
        {"id": "#main/MCSimulation/extra", "source": "#main/more-events"}
    )
    (error,) = errors_of(document)
    assert "are all fed" in error


def test_invalid_feeder():
    document = simulation()
    main_of(document)["inputs"][0][FEEDER_HINT] = {"args": {}}
    assert errors_of(document) == ["inputs.events.dirac:Feeder.name: Field required"]


def test_repeated_hint_class():
    """A hint class appears at most once (DX-ADR-007), the first is not overridden."""
    document = simulation()
    main_of(document)["hints"].append(
        {"class": WORKGRAPH_HINT, "schema_version": "1.0", "type": "Other"}
    )
    assert errors_of(document) == [
        "workflow: hint dirac:Workgraph appears more than once"
    ]


def test_non_string_hint_class():
    document = simulation()
    main_of(document)["hints"].append({"class": ["a"]})
    assert errors_of(document) == ["workflow: a hint class must be a string, not ['a']"]


def test_repeated_input_id():
    document = load("data-processing.cwl")
    main_of(document)["inputs"].append({"id": "#main/raw-data", "type": "string"})
    assert errors_of(document) == ["inputs: id main/raw-data appears more than once"]


def test_non_string_id():
    document = load("parameters-only.cwl")
    document["inputs"] = [{"id": 7, "type": "string"}]
    assert "inputs: an id must be a string, not 7" in errors_of(document)


def test_repeated_step_input_id():
    document = load("data-processing.cwl")
    step_inputs = step_of(document)["in"]
    step_inputs.append(copy.deepcopy(step_inputs[0]))
    assert errors_of(document) == [
        "steps.sprucing.in: id main/sprucing/input-data appears more than once"
    ]


def test_errors_are_collected():
    document = simulation()
    del hint(main_of(document), WORKGRAPH_HINT)["type"]
    del step_of(document)["hints"]
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
    del step_of(document)["hints"]
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
