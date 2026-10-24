from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from diracx.cli.cwl import DocumentLoadError, load_document
from diracx.core.cwl import decompose, format_reference

TOOL = """\
cwlVersion: v1.2
class: CommandLineTool
baseCommand: [echo]
inputs:
  flag:
    type: string
    default: yes
  date:
    type: string
    default: 2026-10-19
outputs: {}
"""


def workflow(run: str) -> str:
    return f"""\
cwlVersion: v1.2
class: Workflow
inputs: {{}}
outputs: {{}}
steps:
  s0:
    run: {run}
    in: {{}}
    out: []
"""


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "echo.cwl").write_text(TOOL)
    (tmp_path / "main.cwl").write_text(workflow("tools/echo.cwl"))
    return tmp_path


def test_run_paths_are_inlined_relative_to_the_referring_file(tree: Path):
    loaded = load_document(tree / "main.cwl")
    assert loaded["steps"]["s0"]["run"]["baseCommand"] == ["echo"]


def test_yaml_is_read_as_1_2(tree: Path):
    inputs = load_document(tree / "tools" / "echo.cwl")["inputs"]
    assert inputs["flag"]["default"] == "yes"
    assert inputs["date"]["default"] == "2026-10-19"


def test_the_same_files_elsewhere_have_the_same_identity(tree: Path, tmp_path_factory):
    elsewhere = tmp_path_factory.mktemp("elsewhere")
    for path in tree.rglob("*.cwl"):
        target = elsewhere / path.relative_to(tree)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(path.read_text())
    assert (
        decompose(load_document(tree / "main.cwl"))[0]
        == decompose(load_document(elsewhere / "main.cwl"))[0]
    )


def test_include_and_import_are_inlined(tmp_path: Path):
    (tmp_path / "script.sh").write_text("echo hello\n")
    (tmp_path / "env.yaml").write_text("HELLO: world\n")
    (tmp_path / "tool.cwl").write_text(
        """\
cwlVersion: v1.2
class: CommandLineTool
requirements:
  InitialWorkDirRequirement:
    listing:
      - entryname: script.sh
        entry: {$include: script.sh}
  EnvVarRequirement:
    envDef: {$import: env.yaml}
baseCommand: [sh, script.sh]
inputs: {}
outputs: {}
"""
    )
    loaded = load_document(tmp_path / "tool.cwl")
    requirements = loaded["requirements"]
    assert (
        requirements["InitialWorkDirRequirement"]["listing"][0]["entry"]
        == "echo hello\n"
    )
    assert requirements["EnvVarRequirement"]["envDef"] == {"HELLO": "world"}


def test_packed_documents_resolve_their_fragments(tmp_path: Path):
    tool = {
        "class": "CommandLineTool",
        "id": "#echo",
        "baseCommand": ["echo"],
        "inputs": {},
        "outputs": {},
    }
    main = {
        "class": "Workflow",
        "id": "#main",
        "inputs": {},
        "outputs": {},
        "steps": {"s0": {"run": "#echo", "in": {}, "out": []}},
    }
    (tmp_path / "packed.cwl").write_text(
        json.dumps({"cwlVersion": "v1.2", "$graph": [main, tool]})
    )
    loaded = load_document(tmp_path / "packed.cwl")
    assert loaded["cwlVersion"] == "v1.2"
    assert loaded["steps"]["s0"]["run"]["baseCommand"] == ["echo"]
    assert load_document(f"{tmp_path / 'packed.cwl'}")["class"] == "Workflow"


def test_a_process_from_a_graph_keeps_the_graph_namespaces(tmp_path: Path):
    (tmp_path / "packed.cwl").write_text(
        json.dumps(
            {
                "cwlVersion": "v1.2",
                "$namespaces": {"edam": "http://edamontology.org/"},
                "$graph": [
                    {
                        "class": "CommandLineTool",
                        "id": "#main",
                        "baseCommand": ["echo"],
                        "inputs": {},
                        "outputs": {},
                    }
                ],
            }
        )
    )
    loaded = load_document(tmp_path / "packed.cwl")
    assert loaded["$namespaces"] == {"edam": "http://edamontology.org/"}


def test_stored_process_references_are_kept(tmp_path: Path):
    reference = format_reference(b"\x01" * 32)
    (tmp_path / "main.cwl").write_text(workflow(reference))
    assert load_document(tmp_path / "main.cwl")["steps"]["s0"]["run"] == reference


def test_reference_cycles_are_refused(tmp_path: Path):
    (tmp_path / "a.cwl").write_text(workflow("b.cwl"))
    (tmp_path / "b.cwl").write_text(workflow("a.cwl"))
    with pytest.raises(DocumentLoadError, match="cycle"):
        load_document(tmp_path / "a.cwl")


def test_nesting_is_bounded(tmp_path: Path):
    for i in range(5):
        (tmp_path / f"w{i}.cwl").write_text(workflow(f"w{i + 1}.cwl"))
    (tmp_path / "w5.cwl").write_text(TOOL)
    load_document(tmp_path / "w0.cwl", max_depth=6)
    with pytest.raises(DocumentLoadError, match="deeper"):
        load_document(tmp_path / "w0.cwl", max_depth=5)


@pytest.mark.parametrize(
    "run, message",
    [
        ("missing.cwl", "Cannot read"),
        ("https://example.org/tool.cwl", "Only local files"),
        ("#nowhere", "no process"),
    ],
)
def test_unloadable_references(tmp_path: Path, run: str, message: str):
    (tmp_path / "main.cwl").write_text(workflow(f'"{run}"'))
    with pytest.raises(DocumentLoadError, match=message):
        load_document(tmp_path / "main.cwl")


def test_large_files_are_refused(tree: Path):
    with pytest.raises(DocumentLoadError, match="larger"):
        load_document(tree / "main.cwl", max_file_bytes=10)


def test_run_is_a_reference_only_on_a_workflow_step(tmp_path: Path):
    (tmp_path / "simulation").write_text("not a process\n")
    (tmp_path / "tool.cwl").write_text(
        """\
cwlVersion: v1.2
class: CommandLineTool
baseCommand: [echo]
inputs:
  run: string
  conf:
    type: Any
    default: {run: simulation}
outputs: {}
"""
    )
    (tmp_path / "main.cwl").write_text(workflow("tool.cwl"))
    loaded = load_document(tmp_path / "main.cwl")
    tool = loaded["steps"]["s0"]["run"]
    assert tool["inputs"]["run"] == "string"
    assert tool["inputs"]["conf"]["default"] == {"run": "simulation"}


def test_runs_in_imported_steps_are_resolved_against_the_imported_file(
    tmp_path: Path,
):
    (tmp_path / "parts" / "tools").mkdir(parents=True)
    (tmp_path / "parts" / "tools" / "echo.cwl").write_text(TOOL)
    (tmp_path / "parts" / "steps.yml").write_text(
        "s0: {run: tools/echo.cwl, in: {}, out: []}\n"
    )
    (tmp_path / "parts" / "step.yml").write_text(
        "{id: s1, run: tools/echo.cwl, in: {}, out: []}\n"
    )
    for name, steps in [
        ("map.cwl", "{$import: parts/steps.yml}"),
        ("list.cwl", "[{$import: parts/step.yml}]"),
    ]:
        (tmp_path / name).write_text(
            f"cwlVersion: v1.2\nclass: Workflow\ninputs: {{}}\noutputs: {{}}\n"
            f"steps: {steps}\n"
        )
    by_map = load_document(tmp_path / "map.cwl")["steps"]["s0"]["run"]
    by_list = load_document(tmp_path / "list.cwl")["steps"][0]["run"]
    assert by_map["baseCommand"] == by_list["baseCommand"] == ["echo"]


def test_list_form_steps_and_inline_run_bodies_are_resolved(tmp_path: Path):
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "echo.cwl").write_text(TOOL)
    (tmp_path / "main.cwl").write_text(
        """\
cwlVersion: v1.2
class: Workflow
inputs: {}
outputs: {}
steps:
  - id: outer
    in: {}
    out: []
    run:
      class: Workflow
      inputs: {}
      outputs: {}
      steps:
        - id: inner
          run: tools/echo.cwl
          in: {}
          out: []
"""
    )
    loaded = load_document(tmp_path / "main.cwl")
    inner = loaded["steps"][0]["run"]["steps"][0]["run"]
    assert inner["baseCommand"] == ["echo"]


def test_a_file_uri_is_accepted(tree: Path):
    assert load_document((tree / "main.cwl").as_uri())["class"] == "Workflow"


@pytest.mark.parametrize(
    "files, entry, message",
    [
        ({"main.cwl": "- just\n- a list\n"}, "main.cwl", "not a CWL document"),
        (
            {"main.cwl": json.dumps({"cwlVersion": "v1.2", "$graph": []})},
            "main.cwl",
            "has no process #main",
        ),
        (
            {"main.cwl": workflow("tool.cwl#other"), "tool.cwl": TOOL},
            "main.cwl",
            "#other",
        ),
        ({"main.cwl": workflow("'#missing'")}, "main.cwl", "has no process #missing"),
        ({"main.cwl": workflow("42")}, "main.cwl", "Invalid run"),
        (
            {
                "a.yml": '{"$import": "b.yml"}\n',
                "b.yml": '{"$import": "a.yml"}\n',
                "main.cwl": TOOL + "hints:\n  - $import: a.yml\n",
            },
            "main.cwl",
            "Cannot \\$import",
        ),
    ],
)
def test_broken_references_are_refused(tmp_path: Path, files, entry, message):
    for name, content in files.items():
        (tmp_path / name).write_text(content)
    with pytest.raises(DocumentLoadError, match=message):
        load_document(tmp_path / entry)


def test_references_inside_a_graph_are_bounded(tmp_path: Path):
    def entry(i: int) -> dict:
        return {
            "class": "Workflow",
            "id": f"#w{i}",
            "inputs": {},
            "outputs": {},
            "steps": {"s": {"run": f"#w{i + 1}", "in": {}, "out": []}},
        }

    graph = [entry(i) for i in range(4)]
    graph[0]["id"] = "#main"
    (tmp_path / "packed.cwl").write_text(
        json.dumps({"cwlVersion": "v1.2", "$graph": [*graph, entry(4)]})
    )
    with pytest.raises(DocumentLoadError, match="deeper than 3"):
        load_document(tmp_path / "packed.cwl", max_depth=3)


FIXTURE = Path(__file__).parent / "fixtures" / "mc-simulation"


def transformations(root: Path) -> dict[str, dict]:
    workgraph = load_document(root / "mc-simulation.cwl")
    return {name: step["run"] for name, step in workgraph["steps"].items()}


def test_the_mc_simulation_workgraph_resolves_to_three_transformations():
    runs = transformations(FIXTURE)
    assert set(runs) == {"MCSimulation", "MCReconstruction", "MCMerge"}
    shared = set.intersection(*(set(decompose(r)[1]) for r in runs.values()))
    assert len(shared) == 1, "lb-run-app.cwl is one process in all three"


def test_identities_do_not_depend_on_where_the_files_are(tmp_path: Path):
    copy = tmp_path / "somewhere" / "else"
    shutil.copytree(FIXTURE, copy)
    here = {n: decompose(r)[0] for n, r in transformations(FIXTURE).items()}
    there = {n: decompose(r)[0] for n, r in transformations(copy).items()}
    assert here == there


def test_a_change_to_the_shared_tool_changes_every_transformation(tmp_path: Path):
    copy = tmp_path / "edited"
    shutil.copytree(FIXTURE, copy)
    tool = copy / "tools" / "lb-run-app.cwl"
    tool.write_text(tool.read_text().replace("lb-prod-run", "lb-prod-run --verbose"))
    before = {n: decompose(r)[0] for n, r in transformations(FIXTURE).items()}
    after = {n: decompose(r)[0] for n, r in transformations(copy).items()}
    assert all(before[n] != after[n] for n in before)
