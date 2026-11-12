"""Tests for workgraph compilation and submission (DX-ADR-007)."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import func, select

from diracx.core.exceptions import InvalidWorkgraphError
from diracx.core.hashing import hash_json
from diracx.core.models.transformation import (
    TransformationKind,
    TransformationStatus,
    WorkgraphStatus,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    Actions,
    ComputeTransformations,
    Ownership,
    Processes,
    RequirementsTemplates,
    TransformationLog,
    TransformationOutputs,
    Transformations,
    WorkgraphLog,
    Workgraphs,
)
from diracx.logic.transformations import compile_workgraph, submit_workgraph
from diracx.logic.transformations.workgraphs import EDGE_FEEDER, PROCESS_URI_PREFIX

EXAMPLES = Path(__file__).parents[3] / "docs" / "assets" / "examples"


def tool(name: str, inputs: list[str], outputs: list[str]) -> dict[str, Any]:
    """A minimal CommandLineTool standing in for a step's real process."""
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": ["echo", name],
        "inputs": {i: {"type": ["File", "string"]} for i in inputs},
        "outputs": {
            o: {"type": "File[]", "outputBinding": {"glob": f"{o}*"}} for o in outputs
        },
    }


def load_example(name: str) -> dict[str, Any]:
    """Load a documented example, with each relative run inlined.

    The documented examples reference their processes by relative path,
    which needs the process store of DIRACGrid/diracx#1080; inlining a tool
    with the same interface keeps everything else as documented.
    """
    document = yaml.safe_load((EXAMPLES / name).read_text())
    for step_name, step in document["steps"].items():
        if isinstance(step.get("run"), str):
            step["run"] = tool(step_name, list(step["in"]), list(step["out"]))
    return document


def packed_simulation() -> dict[str, Any]:
    """A two-step simulation in the packed form ``cwltool --pack`` emits."""
    return {
        "cwlVersion": "v1.2",
        "$namespaces": {"dirac": "https://diracgrid.org/cwl#"},
        "$graph": [
            {
                "class": "Workflow",
                "id": "#main",
                "label": "Packed simulation",
                "hints": [
                    {
                        "class": "dirac:Workgraph",
                        "schema_version": "1.0",
                        "type": "MCSimulation",
                        "target": {"output": "datasets", "files": 2},
                        "approving": [{"action": "ManualApproval"}],
                    }
                ],
                "inputs": [
                    {
                        "id": "#main/events",
                        "type": {"type": "array", "items": ["File", "string"]},
                        "dirac:Feeder": {
                            "name": "Seeds",
                            "args": {"target_events": 2000, "events_per_seed": 1000},
                        },
                    },
                    {"id": "#main/config", "type": "string"},
                ],
                "steps": [
                    {
                        "id": "#main/sim",
                        "hints": [
                            {
                                "class": "dirac:Transformation",
                                "packer": {"name": "PerInput"},
                                "requirements_template": {"priority": 2},
                            }
                        ],
                        "run": "#gauss",
                        "in": [
                            {"id": "#main/sim/seed", "source": "#main/events"},
                            {"id": "#main/sim/config", "source": "#main/config"},
                        ],
                        "out": ["#main/sim/sim-files"],
                    },
                    {
                        "id": "#main/reco",
                        "hints": [
                            {
                                "class": "dirac:Transformation",
                                "packer": {
                                    "name": "FixedSize",
                                    "args": {"group_size": 2},
                                },
                                "actions": {
                                    "Finalizing": [
                                        {"action": "NoInputProcessedTwice"},
                                        {
                                            "action": "MergeHistograms",
                                            "args": {"output": "HIST"},
                                        },
                                    ]
                                },
                            }
                        ],
                        "run": "#reco-workflow",
                        "in": [
                            {
                                "id": "#main/reco/input-data",
                                "source": "#main/sim/sim-files",
                            }
                        ],
                        "out": [{"id": "#main/reco/reco-files"}],
                    },
                ],
                "outputs": [
                    {
                        "id": "#main/datasets",
                        "type": "File[]",
                        "outputSource": "#main/reco/reco-files",
                    }
                ],
            },
            {
                "class": "CommandLineTool",
                "id": "#gauss",
                "baseCommand": ["Gauss"],
                "inputs": [
                    {"id": "#gauss/seed", "type": ["File", "string"]},
                    {"id": "#gauss/config", "type": "string"},
                ],
                "outputs": [{"id": "#gauss/sim-files", "type": "File[]"}],
            },
            {
                # A nested workflow whose step references another graph entry
                "class": "Workflow",
                "id": "#reco-workflow",
                "inputs": [{"id": "#reco-workflow/input-data", "type": "File[]"}],
                "steps": [
                    {
                        "id": "#reco-workflow/boole",
                        "run": "#boole",
                        "in": [
                            {
                                "id": "#reco-workflow/boole/input-data",
                                "source": "#reco-workflow/input-data",
                            }
                        ],
                        "out": ["#reco-workflow/boole/reco-files"],
                    }
                ],
                "outputs": [
                    {
                        "id": "#reco-workflow/reco-files",
                        "type": "File[]",
                        "outputSource": "#reco-workflow/boole/reco-files",
                    }
                ],
            },
            {
                "class": "CommandLineTool",
                "id": "#boole",
                "baseCommand": ["Boole"],
                "inputs": [{"id": "#boole/input-data", "type": "File[]"}],
                "outputs": [{"id": "#boole/reco-files", "type": "File[]"}],
            },
        ],
    }


def by_name(compiled) -> dict[str, Any]:
    return {compiled.names[t.transformation_id]: t for t in compiled.transformations}


# --------------------------------------------------------------------------
# Compilation
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "example", sorted(p.name for p in EXAMPLES.glob("*.cwl")), ids=str
)
def test_documented_examples_compile(example):
    """Every documented workgraph compiles, one transformation per member."""
    document = load_example(example)
    compiled = compile_workgraph(document)

    hint = document["hints"]["dirac:Workgraph"]
    assert compiled.type == hint["type"]
    names = list(by_name(compiled))
    expected = list(document["steps"]) + [
        dm["id"] for dm in hint.get("data_management", [])
    ]
    assert names == expected
    # Every step's run is replaced by the hash of its stored process
    for step_name, step in compiled.stripped_spec["steps"].items():
        assert step["run"].startswith(PROCESS_URI_PREFIX)
        process_id = bytes.fromhex(step["run"].removeprefix(PROCESS_URI_PREFIX))
        assert compiled.processes[process_id] == document["steps"][step_name]["run"]
    # The submitted document is left untouched
    assert document == load_example(example)


def test_simulation_rows():
    """The documented simulation becomes a feeder chain over declared outputs."""
    compiled = compile_workgraph(load_example("simulation.cwl"))
    members = by_name(compiled)

    sim, reco, merge = (
        members["MCSimulation"],
        members["MCReconstruction"],
        members["MCMerge"],
    )
    assert all(t.kind == TransformationKind.COMPUTE for t in members.values())
    assert sim.feeder == "Seeds"
    assert sim.feeder_args["events_per_seed"] == 1000
    assert sim.packer == "PerInput"
    assert reco.packer == "ByGroupSizeRun"
    assert reco.packer_args == {"group_size": 3, "keep_storage_together": True}

    outputs = {
        (compiled.names[o.transformation_id], o.name): o for o in compiled.outputs
    }
    assert set(outputs) == {
        ("MCSimulation", "sim-files"),
        ("MCReconstruction", "reco-files"),
        ("MCMerge", "merged"),
    }
    # Only the merged files are a workgraph output
    assert outputs["MCSimulation", "sim-files"].intermediate
    assert outputs["MCReconstruction", "reco-files"].intermediate
    assert not outputs["MCMerge", "merged"].intermediate
    # Internal edges are served by the edge feeder over the upstream output
    assert reco.feeder == EDGE_FEEDER
    assert reco.feeder_args == {
        "transformation_output_id": str(
            outputs["MCSimulation", "sim-files"].transformation_output_id
        )
    }
    assert merge.feeder_args == {
        "transformation_output_id": str(
            outputs["MCReconstruction", "reco-files"].transformation_output_id
        )
    }
    # One Finalizing action each, on its own transformation
    assert sorted(
        (compiled.names[a.transformation_id], a.state, a.position, a.action)
        for a in compiled.actions
    ) == [
        ("MCMerge", "Finalizing", 0, "NoInputProcessedTwice"),
        ("MCReconstruction", "Finalizing", 0, "NoInputProcessedTwice"),
        ("MCSimulation", "Finalizing", 0, "NoInputProcessedTwice"),
    ]
    assert compiled.annotations["label"] == "MC simulation"
    assert members["MCSimulation"].annotations == {
        "name": "MCSimulation",
        "label": "MCSimulation",
        "doc": "Gauss, one seed per job",
    }


def test_mc_simulation_data_management_and_approving():
    """Data-management declarations become data transformations."""
    compiled = compile_workgraph(load_example("mc-simulation.cwl"))
    members = by_name(compiled)
    outputs = {
        (compiled.names[o.transformation_id], o.name): o.transformation_output_id
        for o in compiled.outputs
    }

    removal = members["MCSimulationRemoval"]
    assert removal.kind == TransformationKind.DATA
    assert removal.process_id is None
    assert removal.feeder == EDGE_FEEDER
    assert removal.feeder_args == {
        "transformation_output_id": str(outputs["MCSimulation", "sim-files"])
    }
    assert removal.annotations["data_management"] == {"operation": "remove"}

    # A declaration against a workflow output reads its output source
    replication = members["OutputReplication"]
    assert replication.feeder_args == {
        "transformation_output_id": str(outputs["MCMerge", "merged"])
    }
    assert replication.annotations["data_management"] == {
        "operation": "replicate",
        "destination": ["CERN-DST", "RAL-DST"],
    }

    approving = [a for a in compiled.actions if a.workgraph_id is not None]
    assert [(a.state, a.position, a.action) for a in approving] == [
        ("Approving", 0, "CheckSuccessRate"),
        ("Approving", 1, "EstimateResourceUsage"),
        ("Approving", 2, "PPGApproval"),
        ("Approving", 3, "ManualApproval"),
    ]
    assert all(a.transformation_id is None for a in approving)
    assert sum(1 for a in compiled.actions if a.transformation_id is not None) == 11


def test_rdst_stripping_data_management_from_workflow_input():
    """A declaration against a workflow input shares that input's feeder."""
    document = load_example("rdst-stripping.cwl")
    compiled = compile_workgraph(document)
    members = by_name(compiled)
    for name in ("RDSTStaging", "BufferRemoval"):
        assert members[name].feeder == members["stripping"].feeder
        assert members[name].feeder_args == members["stripping"].feeder_args


def test_joining_driving_input():
    """With several fed inputs, driving_input picks the one that feeds."""
    compiled = compile_workgraph(load_example("joining.cwl"))
    members = by_name(compiled)
    outputs = {
        (compiled.names[o.transformation_id], o.name): o.transformation_output_id
        for o in compiled.outputs
    }
    step = load_example("joining.cwl")["steps"]["comparison"]
    driving = step["hints"]["dirac:Transformation"]["driving_input"]
    upstream, out = step["in"][driving].split("/")
    assert members["comparison"].feeder_args == {
        "transformation_output_id": str(outputs[upstream, out])
    }


def test_packed_document():
    """A packed document resolves #fragments, nested ones included."""
    document = packed_simulation()
    original = copy.deepcopy(document)
    compiled = compile_workgraph(document)
    assert document == original

    members = by_name(compiled)
    assert list(members) == ["sim", "reco"]
    sim, reco = members["sim"], members["reco"]

    # The non-fed input is a job parameter, not a feeder
    assert sim.feeder == "Seeds"
    assert sim.feeder_args == {"target_events": 2000, "events_per_seed": 1000}
    assert reco.feeder == EDGE_FEEDER

    # The requirements template is stored content-addressed
    assert sim.requirements_template_id == hash_json({"priority": 2})
    assert compiled.requirements_templates == {
        hash_json({"priority": 2}): {"priority": 2}
    }
    assert reco.requirements_template_id is None

    # The nested workflow is stored with its own reference inlined
    reco_process = compiled.processes[reco.process_id]
    assert reco_process["class"] == "Workflow"
    assert reco_process["steps"][0]["run"]["baseCommand"] == ["Boole"]

    # The stripped spec is #main alone, with the namespaces and version kept
    stripped = compiled.stripped_spec
    assert "$graph" not in stripped
    assert stripped["id"] == "#main"
    assert stripped["cwlVersion"] == "v1.2"
    assert stripped["$namespaces"] == {"dirac": "https://diracgrid.org/cwl#"}
    assert [s["run"] for s in stripped["steps"]] == [
        f"{PROCESS_URI_PREFIX}{sim.process_id.hex()}",
        f"{PROCESS_URI_PREFIX}{reco.process_id.hex()}",
    ]

    assert [(a.state, a.position, a.action) for a in compiled.actions] == [
        ("Finalizing", 0, "NoInputProcessedTwice"),
        ("Finalizing", 1, "MergeHistograms"),
        ("Approving", 0, "ManualApproval"),
    ]


def test_hint_forms_are_equivalent():
    """Hints written as a list of classes mean the same as a mapping."""
    as_map = load_example("simulation.cwl")
    as_list = copy.deepcopy(as_map)
    as_list["hints"] = [
        {"class": cls, **body} for cls, body in as_list["hints"].items()
    ]
    for step in as_list["steps"].values():
        step["hints"] = [{"class": cls, **body} for cls, body in step["hints"].items()]

    def rows(compiled):
        return [
            (compiled.names[t.transformation_id], t.feeder, t.packer, t.packer_args)
            for t in compiled.transformations
        ]

    assert rows(compile_workgraph(as_map)) == rows(compile_workgraph(as_list))


def test_identical_processes_are_stored_once():
    """Two steps running the same process share one Processes row."""
    document = load_example("simulation.cwl")
    shared = tool("shared", ["input-data"], ["reco-files"])
    document["steps"]["MCReconstruction"]["run"] = shared
    document["steps"]["MCMerge"]["run"] = copy.deepcopy(shared)
    document["steps"]["MCMerge"]["out"] = ["reco-files"]
    document["outputs"]["datasets"]["outputSource"] = "MCMerge/reco-files"
    compiled = compile_workgraph(document)
    members = by_name(compiled)
    assert members["MCReconstruction"].process_id == members["MCMerge"].process_id
    assert len(compiled.processes) == 2


# --------------------------------------------------------------------------
# Invalid documents
# --------------------------------------------------------------------------


def errors_of(document: Any) -> list[str]:
    with pytest.raises(InvalidWorkgraphError) as exc_info:
        compile_workgraph(document)
    return exc_info.value.errors


def simulation() -> dict[str, Any]:
    return load_example("simulation.cwl")


def test_not_an_object():
    assert errors_of([]) == ["the document must be a JSON object"]


def test_wrong_cwl_version():
    document = simulation()
    document["cwlVersion"] = "v1.0"
    assert errors_of(document) == ["cwlVersion must be v1.2, not 'v1.0'"]


def test_not_a_workflow():
    document = simulation()
    document["class"] = "CommandLineTool"
    assert any("must be a Workflow" in e for e in errors_of(document))


def test_missing_workgraph_hint():
    document = simulation()
    del document["hints"]["dirac:Workgraph"]
    assert errors_of(document) == ["the workflow must carry the dirac:Workgraph hint"]


def test_unknown_hint_field():
    document = simulation()
    document["hints"]["dirac:Workgraph"]["colour"] = "blue"
    assert errors_of(document) == [
        "dirac:Workgraph.colour: Extra inputs are not permitted"
    ]


def test_unsupported_schema_version():
    document = simulation()
    document["hints"]["dirac:Workgraph"]["schema_version"] = "2.0"
    (error,) = errors_of(document)
    assert error.startswith("dirac:Workgraph.schema_version:")


def test_missing_type():
    document = simulation()
    del document["hints"]["dirac:Workgraph"]["type"]
    assert errors_of(document) == ["dirac:Workgraph.type is required"]


def test_bare_step():
    document = simulation()
    del document["steps"]["MCMerge"]["hints"]
    (error,) = errors_of(document)
    assert error.startswith("steps.MCMerge: every step of a workgraph must carry")


def test_invalid_transformation_hint():
    document = simulation()
    document["steps"]["MCMerge"]["hints"]["dirac:Transformation"]["packer"] = {}
    assert errors_of(document) == ["steps.MCMerge.hints.packer.name: Field required"]


def test_missing_packer():
    document = simulation()
    del document["steps"]["MCMerge"]["hints"]["dirac:Transformation"]["packer"]
    assert errors_of(document) == [
        "steps.MCMerge.hints.dirac:Transformation.packer is required"
    ]


def test_relative_run_reference():
    document = simulation()
    document["steps"]["MCMerge"]["run"] = "transformations/merge.cwl"
    (error,) = errors_of(document)
    assert error.startswith("steps.MCMerge.run: run must be an inline process")


def test_missing_fragment():
    document = packed_simulation()
    document["$graph"][0]["steps"][0]["run"] = "#nowhere"
    assert errors_of(document) == ["steps.sim.run: run #nowhere is not in the $graph"]


def test_packed_without_main():
    document = packed_simulation()
    document["$graph"][0]["id"] = "#other"
    assert errors_of(document) == ["a packed document must have a #main process"]


def test_unconsumed_output():
    document = simulation()
    document["steps"]["MCMerge"]["out"].append("logs")
    assert errors_of(document) == [
        "steps.MCMerge.out.logs: an output must be consumed by a step or "
        "declared as a workflow output"
    ]


def test_unknown_source():
    document = simulation()
    document["steps"]["MCMerge"]["in"]["input-data"] = "MCReconstruction/nothing"
    errors = errors_of(document)
    assert (
        "steps.MCMerge.in.input-data: source MCReconstruction/nothing is neither "
        "a workflow input nor a step output"
    ) in errors


def test_cycle():
    document = simulation()
    document["steps"]["MCSimulation"]["in"] = {"seed": "MCMerge/merged"}
    document["steps"]["MCMerge"]["out"] = ["merged"]
    assert any("the dataflow has a cycle" in e for e in errors_of(document))


def test_target_must_be_an_output():
    document = simulation()
    document["hints"]["dirac:Workgraph"]["target"]["output"] = "nothing"
    assert errors_of(document) == [
        "dirac:Workgraph.target.output nothing is not a workflow output"
    ]


def test_unknown_action_state():
    document = simulation()
    hint = document["steps"]["MCMerge"]["hints"]["dirac:Transformation"]
    hint["actions"] = {"Sleeping": [{"action": "Snore"}]}
    assert errors_of(document) == [
        "steps.MCMerge.actions: Sleeping is not a transformation state"
    ]


def test_ambiguous_fed_inputs():
    document = load_example("joining.cwl")
    del document["steps"]["comparison"]["hints"]["dirac:Transformation"][
        "driving_input"
    ]
    (error,) = errors_of(document)
    assert "set driving_input" in error


def test_unknown_after():
    document = simulation()
    hint = document["steps"]["MCMerge"]["hints"]["dirac:Transformation"]
    hint["after"] = ["Nobody"]
    assert errors_of(document) == ["MCMerge: after Nobody is not a transformation"]


def test_data_management_on_unknown_files():
    document = load_example("mc-simulation.cwl")
    document["hints"]["dirac:Workgraph"]["data_management"][0]["files"] = "nothing"
    (error,) = errors_of(document)
    assert error.startswith("dirac:Workgraph.data_management.MCSimulationRemoval:")


def test_duplicate_names():
    document = load_example("mc-simulation.cwl")
    document["hints"]["dirac:Workgraph"]["data_management"][0]["id"] = "MCMerge"
    assert errors_of(document) == ["transformation name MCMerge is used twice"]


def test_errors_are_collected():
    """Independent problems are all reported, not only the first."""
    document = simulation()
    document["steps"]["MCMerge"]["out"].append("logs")
    document["hints"]["dirac:Workgraph"]["target"]["output"] = "nothing"
    assert len(errors_of(document)) == 2


# --------------------------------------------------------------------------
# Submission
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


async def test_submit_workgraph(transformation_db):
    """Submission stores every definition-side row, all New."""
    async with transformation_db as db:
        result = await submit_workgraph(
            load_example("mc-simulation.cwl"),
            transformation_db=db,
            vo="lhcb",
            sub="lhcb:someone",
            author="someone",
        )

    assert [(t.name, t.kind) for t in result.transformations] == [
        ("MCSimulation", "Compute"),
        ("MCReconstruction", "Compute"),
        ("MCMerge", "Compute"),
        ("MCSimulationRemoval", "Data"),
        ("MCReconstructionRemoval", "Data"),
        ("OutputReplication", "Data"),
    ]

    async with transformation_db as db:
        workgraph = (await db.conn.execute(select(Workgraphs))).one()
        assert str(workgraph.WorkgraphID) == str(result.workgraph_id)
        assert workgraph.Status == WorkgraphStatus.NEW
        assert workgraph.Type == "MCSimulation"
        assert workgraph.StrippedSpec["steps"]["MCSimulation"]["run"].startswith(
            PROCESS_URI_PREFIX
        )

        ownership = (await db.conn.execute(select(Ownership))).one()
        assert (ownership.VO, ownership.Sub) == ("lhcb", "lhcb:someone")
        assert ownership.WorkgraphID == workgraph.WorkgraphID

        transformations = (await db.conn.execute(select(Transformations))).all()
        assert len(transformations) == 6
        assert {t.Status for t in transformations} == {TransformationStatus.NEW}
        assert {t.OwnershipID for t in transformations} == {ownership.OwnershipID}

        compute = (await db.conn.execute(select(ComputeTransformations))).all()
        assert len(compute) == 3
        process_ids = {row.ProcessID for row in compute}
        stored = set((await db.conn.execute(select(Processes.process_id))).scalars())
        assert process_ids == stored

        assert await count(db, TransformationOutputs) == 3
        assert await count(db, Actions) == 15
        assert await count(db, RequirementsTemplates) == 0
        assert await count(db, WorkgraphLog) == 1
        assert await count(db, TransformationLog) == 6


async def test_resubmission_reuses_processes(transformation_db):
    """A second submission is a new workgraph over the same stored processes."""
    document = packed_simulation()
    for _ in range(2):
        async with transformation_db as db:
            await submit_workgraph(
                document, transformation_db=db, vo="lhcb", sub="s", author="a"
            )
    async with transformation_db as db:
        assert await count(db, Workgraphs) == 2
        assert await count(db, Transformations) == 4
        assert await count(db, Processes) == 2
        assert await count(db, RequirementsTemplates) == 1


async def test_invalid_submission_stores_nothing(transformation_db):
    document = simulation()
    del document["steps"]["MCMerge"]["hints"]
    async with transformation_db as db:
        with pytest.raises(InvalidWorkgraphError):
            await submit_workgraph(
                document, transformation_db=db, vo="lhcb", sub="s", author="a"
            )
    async with transformation_db as db:
        assert await count(db, Workgraphs) == 0
        assert await count(db, Processes) == 0
