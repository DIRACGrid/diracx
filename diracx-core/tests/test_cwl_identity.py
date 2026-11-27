"""Identity of packed CWL processes.

``fixtures/mc-simulation.packed.json`` is the MCSimulation workgraph of #1080
as ``cwltool --pack`` (3.3.20260925135507) writes it.
"""

from __future__ import annotations

import copy
import json
import re
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
from diracx.core.cwl.identity import (
    MAX_INLINE_DEPTH,
    _dollar_keys,
    _file_literals,
    _runs,
)

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


def workflow(run: object, entry_id: str = "#main") -> dict:
    return {
        "class": "Workflow",
        "id": entry_id,
        "inputs": [],
        "outputs": [],
        "steps": [{"id": f"{entry_id}/step", "in": [], "out": [], "run": run}],
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
            lambda d: entry(d, "#merge.cwl")["steps"][0].update(run={"class": "Nope"}),
            "not a process",
        ),
        (
            lambda d: entry(d, "#merge.cwl")["steps"][0].update(run={}),
            "not a process",
        ),
        (
            lambda d: entry(d, "#merge.cwl")["steps"][0].update(run=42),
            "not a process",
        ),
        (
            lambda d: (
                entry(d, TOOL)
                .setdefault("hints", [])
                .append({"class": "dirac:Anything"})
            ),
            "'dirac:Anything' .* uses the \\$namespaces prefix 'dirac'",
        ),
        (
            lambda d: entry(d, TOOL).update({"dirac:extension": 1}),
            "'dirac:extension' .* uses the \\$namespaces prefix 'dirac'",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(format="dirac:fmt"),
            "'dirac:fmt' .* uses the \\$namespaces prefix 'dirac'",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(
                format=["https://example.org/fmt", "dirac:fmt"]
            ),
            "'dirac:fmt' .* uses the \\$namespaces prefix 'dirac'",
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


@pytest.mark.parametrize("run", [{"class": "Nope"}, {}], ids=["unknown-class", "empty"])
def test_inline_bodies_that_are_not_processes_are_not_hashed(packed, run):
    # As for a $graph entry: no identity for something that is not a process
    entry(packed, "#merge.cwl")["steps"][0]["run"] = run
    with pytest.raises(ValueError, match="Not a CWL process"):
        decompose(packed)


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


@pytest.mark.parametrize("inline", [False, True], ids=["entry", "inline-body"])
def test_a_body_declaring_another_version_is_refused(packed, inline):
    # Its version is dropped from the hash, so it would pass for v1.2
    merge = entry(packed, "#merge.cwl")
    if inline:
        step = entry(packed, "#main")["steps"][0]
        step["run"] = merge = copy.deepcopy(merge)
    merge["cwlVersion"] = "v1.0"
    for check in (check_self_contained, decompose):
        with pytest.raises(ValueError, match="declares cwlVersion 'v1.0'"):
            check(packed)
    merge["cwlVersion"] = "v1.2"
    check_self_contained(packed)


@pytest.mark.parametrize(
    "where",
    [
        {"hints": [{"class": "fmt:Hint"}]},
        {"inputs": [{"id": "x", "type": "File", "format": "fmt:example"}]},
        {"inputs": [{"id": "x", "type": "fmt:Colour"}]},
        {"inputs": [{"id": "x", "type": "fmt:Colour[]"}]},
        {"inputs": [{"id": "x", "type": "fmt:Colour?"}]},
        {"inputs": [{"id": "x", "type": ["null", "fmt:Colour"]}]},
        {"inputs": [{"id": "x", "type": {"type": "array", "items": "fmt:Colour"}}]},
        {
            "hints": [
                {
                    "class": "SoftwareRequirement",
                    "packages": [{"package": "gauss", "specs": ["fmt:gauss"]}],
                }
            ]
        },
        {"baseCommand": ["fmt:not-a-name"]},
        {"doc": "fmt: a field CWL does not resolve, refused all the same"},
    ],
    ids=[
        "class",
        "format",
        "type",
        "type-array",
        "type-optional",
        "type-union",
        "items",
        "specs",
        "argument",
        "text",
    ],
)
def test_a_name_whose_meaning_hangs_on_a_binding_is_refused_under_any(where):
    # The identity does not cover $namespaces, so no binding may matter. A
    # binding only changes a string starting "<prefix>:", whatever its field,
    # so every such string is refused, even where CWL would not resolve it
    for iri in ("http://edamontology.org/", "https://example.org/other#"):
        document = {**tool(), **where, "$namespaces": {"fmt": iri}}
        with pytest.raises(NotSelfContainedError, match="prefix 'fmt'"):
            check_self_contained(document)


def _rename_run(d, to):
    step = entry(d, "#merge.cwl")["steps"][0]
    step[to] = step.pop("run")


CWL = "https://w3id.org/cwl/cwl#"


@pytest.mark.parametrize(
    "mutate, message",
    [
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(
                default={"class": "cwl:File", "location": "file:///etc/hosts"}
            ),
            "'cwl:File' .* built-in prefix 'cwl'",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(
                default={"class": f"{CWL}File", "location": "file:///etc/hosts"}
            ),
            "'https://w3id.org/cwl/cwl#File' .* as an IRI",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(
                default={"class": "cwl:Directory", "location": "file:///etc"}
            ),
            "'cwl:Directory' .* built-in prefix 'cwl'",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(
                default={"class": f"{CWL}Directory", "location": "file:///etc"}
            ),
            "'https://w3id.org/cwl/cwl#Directory' .* as an IRI",
        ),
        (lambda d: _rename_run(d, "cwl:run"), "'cwl:run' .* built-in prefix 'cwl'"),
        (lambda d: _rename_run(d, f"{CWL}run"), "'https://w3id.org/cwl/cwl#run'"),
        (
            lambda d: entry(d, TOOL).update({"https://w3id.org/cwl/salad#anything": 1}),
            "salad#anything' .* as an IRI",
        ),
        (
            lambda d: entry(d, TOOL)["inputs"][0].update(type="xsd:string"),
            "'xsd:string' .* built-in prefix 'xsd'",
        ),
    ],
    ids=[
        "File-prefix",
        "File-IRI",
        "Directory-prefix",
        "Directory-IRI",
        "run-prefix",
        "run-IRI",
        "salad-IRI",
        "xsd",
    ],
)
def test_cwl_names_are_only_accepted_bare(packed, mutate, message):
    # A runner reads cwl:File or its IRI as File, and cwl:run as run: other
    # spellings would slip past every check made on the bare names
    mutate(packed)
    with pytest.raises(NotSelfContainedError, match=message):
        check_self_contained(packed)


def test_bad_ids_are_listed_with_every_other_problem(packed):
    # check_self_contained names everything found, ids included
    packed["$schemas"] = ["https://example.org/s.rdf"]
    entry(packed, "#merge.cwl")["steps"][0]["run"] = "merge.cwl"
    packed["$graph"].append(copy.deepcopy(entry(packed, TOOL)))
    nameless = tool("nameless")
    del nameless["id"]
    packed["$graph"].append(nameless)
    with pytest.raises(NotSelfContainedError) as raised:
        check_self_contained(packed)
    for problem in ("Two \\$graph entries", "has no id", "\\$schemas", "merge.cwl"):
        assert re.search(problem, str(raised.value)), problem


def test_strings_no_binding_can_change_are_left_alone(packed):
    # Only a string starting with a prefix of $namespaces is expanded
    entry(packed, TOOL).setdefault("hints", []).append({"class": "other:Thing"})
    entry(packed, TOOL)["doc"] = "Mentions dirac:Transformation mid-sentence"
    entry(packed, TOOL)["baseCommand"] = ["https://example.org/not-bound"]
    check_self_contained(packed)


@pytest.mark.parametrize(
    "wrapper",
    [{"doc": {"$include": "https://example.org/doc.txt"}}, {"label": "MC"}],
    ids=["include", "plain"],
)
def test_a_graph_document_holds_only_what_cwltool_pack_writes(packed, wrapper):
    # Nothing scans the wrapper's other keys, so none may be there
    packed.update(wrapper)
    with pytest.raises(NotSelfContainedError, match=f"top-level {next(iter(wrapper))}"):
        check_self_contained(packed)


def test_a_single_process_subgraph_is_the_document_itself():
    single = {k: v for k, v in tool().items() if k != "id"}
    assert subgraph(single, "#main") == single
    check_self_contained(subgraph(single, "#main"))
    assert entry_identity(subgraph(single, "#main")) == entry_identity(single)


def test_an_empty_steps_map_is_not_an_empty_steps_list():
    def workflow(steps):
        return {"cwlVersion": "v1.2", "class": "Workflow", "steps": steps}

    assert entry_identity(workflow({})) != entry_identity(workflow([]))


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


@pytest.mark.parametrize("reverse", [False, True], ids=["entry-last", "entry-first"])
def test_an_inline_copy_of_an_entry_keeps_the_entry(packed, reverse):
    # Both hash to one identity: whichever is identified last must not drop
    # the entry id, or entry_identity misses an entry the document has
    merge = entry(packed, "#main")["steps"][0]
    entry_id = merge["run"]
    merge["run"] = copy.deepcopy(entry(packed, entry_id))
    if reverse:
        packed["$graph"].reverse()
    assert entry_identity(packed, entry_id) == entry_identity(
        json.loads(FIXTURE.read_text()), entry_id
    )


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


STEPS = "#main: steps at / are not a list or map of objects"
INLINE_STEPS = "#main: steps at /#main/step are not a list or map of objects"


@pytest.mark.parametrize(
    "document, message",
    [
        ([tool()], "Not a CWL document"),
        ("abc", "Not a CWL document"),
        (None, "Not a CWL document"),
        ({"$graph": 5}, r"\$graph is not a list of processes: 5"),
        ({"$graph": None}, r"\$graph is not a list of processes: None"),
        ({"$graph": [{**tool(), "class": ["Workflow"]}]}, "Not a CWL process"),
        ({"$graph": [{**tool(), "class": {}}]}, "Not a CWL process"),
        ({**tool(), "class": []}, "Not a CWL process"),
        ({"$graph": [{**tool(), "id": ["#main"]}]}, "id that is not a string"),
        ({"$graph": [{**tool(), "id": {}}]}, "id that is not a string"),
        ({"$graph": [{**tool(), "id": 5}]}, "id that is not a string"),
        ({**tool(), "id": []}, "id that is not a string"),
        ({**tool(), "$namespaces": 5}, r"\$namespaces is not a map of prefixes: 5"),
        ({**tool(), "$namespaces": [["a"]]}, r"\$namespaces is not a map"),
        ({**tool(), "steps": "abc"}, STEPS),
        ({**tool(), "steps": 5}, STEPS),
        ({**tool(), "steps": True}, STEPS),
        ({**tool(), "steps": ["a"]}, STEPS),
        ({**tool(), "steps": [None]}, STEPS),
        ({**tool(), "steps": [[workflow("#main")["steps"][0]]]}, STEPS),
        ({**tool(), "steps": {"step": "#main"}}, STEPS),
        ({"$graph": [{**tool(), "steps": "abc"}]}, STEPS),
        (workflow({**tool(), "steps": "abc"}), INLINE_STEPS),
        (workflow({**tool(), "steps": 5}), INLINE_STEPS),
        (workflow({**tool(), "steps": ["a"]}), INLINE_STEPS),
        (workflow({**tool(), "steps": {"step": "#main"}}), INLINE_STEPS),
        (workflow({"class": ["Workflow"]}), "run at /#main/step is not a process"),
        (workflow({"class": {}}), "run at /#main/step is not a process"),
    ],
)
def test_documents_without_the_shape_of_a_packed_one_are_refused(document, message):
    # Nothing has validated the document as CWL yet
    if isinstance(document, dict):
        document = {"cwlVersion": "v1.2", **document}
    with pytest.raises(ValueError, match=message):
        check_self_contained(document)


def test_inline_bodies_nest_to_a_bound():
    def nested(depth: int) -> dict:
        process = tool()
        for _ in range(depth):
            process = workflow(process)
        return {**process, "cwlVersion": "v1.2"}

    check_self_contained(nested(MAX_INLINE_DEPTH))
    assert entry_identity(nested(MAX_INLINE_DEPTH))
    for depth in (MAX_INLINE_DEPTH + 1, 5000):
        with pytest.raises(ValueError, match="#main: inline processes are nested"):
            check_self_contained(nested(depth))


@pytest.mark.parametrize("wrap", [lambda v: [v], lambda v: {"a": v}], ids=["[]", "{}"])
def test_a_document_nested_too_deeply_to_be_read_is_refused(wrap):
    value = "x"
    for _ in range(5000):
        value = wrap(value)
    for document in (
        {**tool(), "arguments": [value]},
        {"cwlVersion": "v1.2", "$graph": [value]},
    ):
        with pytest.raises(ValueError, match="nested too deeply to be read"):
            check_self_contained(document)


def test_entries_running_one_another_too_deeply_to_be_hashed_are_refused():
    # Each is a $graph entry, so the document is self-contained
    entries = [workflow(f"#{i + 1}", f"#{i}") for i in range(5000)]
    document = {"cwlVersion": "v1.2", "$graph": [*entries, {**tool(), "id": "#5000"}]}
    check_self_contained(document)
    with pytest.raises(ValueError, match="nested too deeply to be hashed"):
        decompose(document)
    assert subgraph(document, "#0") == document


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
