"""Identity of packed CWL processes.

``fixtures/mc-simulation.packed.json`` is the MCSimulation workgraph of #1080
as ``cwltool --pack`` (3.3.20260925135507) writes it.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from diracx.core.cwl import (
    NotSelfContainedError,
    check_self_contained,
    decompose,
    entry_identity,
    format_reference,
    subgraph,
)
from diracx.core.cwl.identity import _dollar_keys, _file_literals, _runs

FIXTURE = Path(__file__).parent / "fixtures" / "mc-simulation.packed.json"
TRANSFORMATIONS = ("#gauss.cwl", "#boole-moore.cwl", "#merge.cwl")
TOOL = "#lb-run-app.cwl"


@pytest.fixture
def packed() -> dict:
    return json.loads(FIXTURE.read_text())


def entry(document: dict, entry_id: str) -> dict:
    return next(e for e in document["$graph"] if e["id"] == entry_id)


def tool(command: str = "echo") -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "id": "#main",
        "baseCommand": [command],
        "inputs": [],
        "outputs": [],
    }


def test_the_fixture_is_self_contained(packed):
    check_self_contained(packed)


def test_the_same_document_has_the_same_identities(packed):
    reordered = json.loads(json.dumps(packed, sort_keys=True))
    assert decompose(packed) == decompose(reordered)


def test_each_transformation_is_a_tree_sharing_one_tool(packed):
    components = {c.entry_id: c for c in decompose(packed).values()}
    tool_id = components[TOOL].process_id
    assert components["#gauss.cwl"].children == (tool_id,)
    assert components["#merge.cwl"].children == (tool_id,)
    reco = components["#boole-moore.cwl"].children
    assert reco == (components["#digi-reco.cwl"].process_id,)
    assert components["#digi-reco.cwl"].children == (tool_id,)
    workgraph = set(components["#main"].children)
    assert workgraph == {components[t].process_id for t in TRANSFORMATIONS}


def test_a_change_reaches_every_process_that_runs_it_and_nothing_else(packed):
    before = {c.entry_id: c.process_id for c in decompose(packed).values()}
    edited = copy.deepcopy(packed)
    entry(edited, TOOL)["baseCommand"] = ["lb-prod-run", "--verbose"]
    after = {c.entry_id: c.process_id for c in decompose(edited).values()}
    unchanged = {e for e in before if before[e] == after[e]}
    assert unchanged == set()  # every process here runs the tool, directly or not
    edited = copy.deepcopy(packed)
    entry(edited, "#merge.cwl")["doc"] = "Merged differently"
    after = {c.entry_id: c.process_id for c in decompose(edited).values()}
    assert {e for e in before if before[e] != after[e]} == {"#merge.cwl", "#main"}


def test_a_run_is_hashed_as_its_childs_identity(packed):
    merge = entry_identity(packed, "#merge.cwl")
    alone = subgraph(packed, "#merge.cwl")
    assert entry_identity(alone, "#merge.cwl") == merge
    inline = copy.deepcopy(alone)
    step = entry(inline, "#merge.cwl")["steps"][0]
    step["run"] = entry(inline, TOOL)
    inline["$graph"] = [entry(inline, "#merge.cwl")]
    assert entry_identity(inline, "#merge.cwl") == merge, "inline or by #id"


def test_subgraph_holds_an_entry_and_what_it_runs(packed):
    reco = subgraph(packed, "#boole-moore.cwl")
    assert [e["id"] for e in reco["$graph"]] == [
        "#boole-moore.cwl",
        "#digi-reco.cwl",
        TOOL,
    ]
    assert reco["$namespaces"] == packed["$namespaces"]
    check_self_contained(reco)


def test_a_single_process_document_is_its_own_main():
    assert entry_identity(tool()) == entry_identity(
        {"cwlVersion": "v1.2", "$graph": [tool()]}
    )
    assert entry_identity(tool("echo")) != entry_identity(tool("cat"))


def test_a_reference_is_the_childs_hash():
    assert format_reference(b"\xab" * 32) == "proc:sha256-" + "ab" * 32


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda d: d.update({"$schemas": ["https://example.org/s.rdf"]}), r"\$schemas"),
        (
            lambda d: entry(d, TOOL)["requirements"][0].update(
                {"listing": [{"$include": "/etc/hosts"}]}
            ),
            r"\$include",
        ),
        (
            lambda d: entry(d, "#merge.cwl")["steps"][0].update(run="merge.cwl"),
            "not in \\$graph",
        ),
        (
            lambda d: entry(d, "#merge.cwl")["steps"][0].update(run=42),
            "not a process",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(
                default={"class": "File", "path": "/home/someone/data.root"}
            ),
            "File .* names a path or location",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(
                default={"class": "Directory", "location": "https://example.org/d"}
            ),
            "Directory .* names a path or location",
        ),
    ],
)
def test_documents_needing_something_from_outside_are_refused(packed, mutate, message):
    mutate(packed)
    with pytest.raises(NotSelfContainedError, match=message):
        check_self_contained(packed)


def test_file_literals_with_inline_contents_are_allowed(packed):
    entry(packed, TOOL)["inputs"][0]["default"] = {
        "class": "File",
        "basename": "conf.json",
        "contents": "{}",
    }
    check_self_contained(packed)


def test_other_versions_and_non_processes_are_refused(packed):
    with pytest.raises(ValueError, match="cwl-upgrader"):
        decompose({**packed, "cwlVersion": "v1.0"})
    with pytest.raises(ValueError, match="Not a CWL process"):
        decompose({"cwlVersion": "v1.2", "$graph": [{"class": "Nope"}]})


def test_a_process_that_runs_itself_is_refused(packed):
    entry(packed, "#merge.cwl")["steps"][0]["run"] = "#merge.cwl"
    with pytest.raises(ValueError, match="runs itself"):
        decompose(packed)


def test_numbers_canonical_json_cannot_represent_are_refused():
    process = tool()
    process["arguments"] = [2**53]
    with pytest.raises(ValueError, match="#main"):
        decompose(process)


def test_golden_identity(packed):
    # Changing this value means every stored identity changes: plan the
    # migration instead of editing it.
    assert (
        entry_identity(packed, "#merge.cwl").hex()
        == "9d84d8c7268cebe96c2de6e29f010e83a7a5e34a94133b41fedc3a5136aeaf60"
    )


def test_runs_inside_inline_bodies_are_checked_and_hashed(packed):
    workgraph = entry(packed, "#main")
    merge = workgraph["steps"][0]
    merge["run"] = copy.deepcopy(entry(packed, merge["run"]))
    merge["run"]["steps"][0]["run"] = "#missing.cwl"
    with pytest.raises(NotSelfContainedError, match="#missing.cwl"):
        check_self_contained(packed)
    merge["run"]["steps"][0]["run"] = TOOL
    assert decompose(packed) == decompose(json.loads(FIXTURE.read_text())), (
        "an inline copy of an entry is the same process"
    )
    merge["run"]["doc"] = "An inline merge"
    components = decompose(packed)
    tool_id = entry_identity(packed, TOOL)
    inline = [c for c in components.values() if c.entry_id is None]
    assert len(inline) == 1 and inline[0].children == (tool_id,)


def test_steps_written_as_a_map_are_hashed_through_their_runs():
    child = tool()
    child["id"] = "#child"
    parent = {
        "class": "Workflow",
        "id": "#main",
        "inputs": [],
        "outputs": [],
        "steps": {"s0": {"run": "#child", "in": [], "out": []}},
    }
    document = {"cwlVersion": "v1.2", "$graph": [parent, child]}
    components = {c.entry_id: c for c in decompose(document).values()}
    assert components["#main"].children == (components["#child"].process_id,)


def test_an_unknown_entry_is_a_key_error(packed):
    with pytest.raises(KeyError):
        entry_identity(packed, "#nowhere.cwl")


def test_entries_with_a_repeated_id_are_refused(packed):
    # Otherwise the second would silently replace the first
    other = copy.deepcopy(entry(packed, TOOL))
    other["baseCommand"] = ["lb-run", "--verbose", "LHCb/v57r2"]
    packed["$graph"].append(other)
    for check in (check_self_contained, decompose):
        with pytest.raises(
            ValueError, match=f"Two \\$graph entries have the id '{TOOL}'"
        ):
            check(packed)


def test_graph_entries_without_an_id_are_refused():
    first, second = tool("first"), tool("second")
    del first["id"], second["id"]
    document = {"cwlVersion": "v1.2", "$graph": [first, second]}
    for check in (check_self_contained, decompose):
        with pytest.raises(ValueError, match="has no id"):
            check(document)
    # A single process document needs none: it is #main
    assert entry_identity({**first, "cwlVersion": "v1.2"}, "#main")


def test_dollar_keys_are_found_at_any_depth():
    value = {"a": [{"$include": "x"}, {"b": {"$import": "y"}}], "$mixin": {}}
    assert sorted(_dollar_keys(value)) == [
        ("/", "$mixin"),
        ("/a/0", "$include"),
        ("/a/1/b", "$import"),
    ]


def test_file_literals_are_found_at_any_depth():
    file = {"class": "File", "path": "a"}
    directory = {"class": "Directory", "listing": [file]}
    value = {"inputs": [{"default": directory}], "other": {"class": "Workflow"}}
    assert list(_file_literals(value)) == [
        ("/inputs/0/default", directory),
        ("/inputs/0/default/listing/0", file),
    ]


def test_runs_include_those_of_inline_bodies():
    inner = {"class": "Workflow", "steps": [{"id": "#in", "run": "#tool"}]}
    outer = {"class": "Workflow", "steps": {"s": {"id": "#out", "run": inner}}}
    assert list(_runs(outer)) == [("/#out", inner), ("/#out/#in", "#tool")]
