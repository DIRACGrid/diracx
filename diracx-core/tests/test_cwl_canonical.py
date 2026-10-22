from __future__ import annotations

import copy
import json

import pytest

from diracx.core.cwl import (
    canonical_bytes,
    decompose,
    format_reference,
    pack,
    parse_reference,
    unpack,
)


def merge_tool(command: str = "hadd") -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "id": "file:///home/someone/prod/merge.cwl#main",
        "doc": "Merge files",
        "baseCommand": [command],
        "inputs": {"input-data": {"type": "File[]", "inputBinding": {"position": 1}}},
        "outputs": {"merged": {"type": "File", "outputBinding": {"glob": "*.root"}}},
    }


def workflow(*tools: dict) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "id": "transformation_3",
        "label": "MCMerge",
        "hints": [{"class": "dirac:Transformation", "group_size": 4}],
        "inputs": [{"id": "input-data", "type": "File[]"}],
        "outputs": [
            {
                "id": "merged",
                "type": "File",
                "outputSource": f"step{len(tools) - 1}/merged",
            }
        ],
        "steps": [
            {
                "id": f"#transformation_3/step{i}",
                "run": tool,
                "in": {"input-data": "input-data"},
                "out": ["merged"],
            }
            for i, tool in enumerate(tools)
        ],
    }


def root_id(process: dict) -> bytes:
    return decompose(process)[0]


def test_same_document_loaded_from_two_places_has_one_identity():
    a, b = merge_tool(), merge_tool()
    b["id"] = "file:///tmp/elsewhere/merge.cwl#main"
    assert root_id(a) == root_id(b)


def test_identity_ignores_formatting_key_order_and_map_vs_list_form():
    a = merge_tool()
    b = json.loads(json.dumps(a, sort_keys=True))
    b["inputs"] = [
        {"id": "input-data", "inputBinding": {"position": 1}, "type": "File[]"}
    ]
    b["outputs"] = [
        {"id": "merged", "type": "File", "outputBinding": {"glob": "*.root"}}
    ]
    assert root_id(a) == root_id(b)


def test_identity_ignores_metadata_and_dirac_hints():
    a = workflow(merge_tool())
    b = copy.deepcopy(a)
    b["label"] = "something else"
    b["hints"] = [{"class": "dirac:Transformation", "group_size": 8}]
    b["inputs"][0]["dirac:Feeder"] = {"name": "Seeds"}
    assert root_id(a) == root_id(b)


def formatted(prefix: str, uri: str) -> dict:
    tool = merge_tool()
    tool["$namespaces"] = {prefix: uri}
    tool["inputs"]["input-data"]["format"] = f"{prefix}:format_2572"
    return tool


def test_identity_covers_what_a_prefix_means_not_which_prefix():
    edam = "http://edamontology.org/"
    assert root_id(formatted("edam", edam)) == root_id(formatted("e", edam))
    assert root_id(formatted("edam", edam)) != root_id(
        formatted("edam", "http://example.org/")
    )


def test_nested_processes_inherit_and_may_rebind_prefixes():
    inherited = workflow(formatted("edam", "http://edamontology.org/"))
    child = inherited["steps"][0]["run"]
    inherited["$namespaces"] = child.pop("$namespaces")
    root, nodes = decompose(inherited)
    assert root_id(formatted("edam", "http://edamontology.org/")) in nodes
    rebound = copy.deepcopy(inherited)
    rebound["steps"][0]["run"]["$namespaces"] = {"edam": "http://example.org/"}
    assert root_id(rebound) != root


def test_fully_scoped_step_ports_match_the_short_form():
    short = workflow(merge_tool())
    short["steps"][0]["out"] = ["merged"]
    scoped = copy.deepcopy(short)
    scoped["id"] = "file:///home/someone/prod/merge.cwl#transformation_3"
    scope = "file:///home/someone/prod/merge.cwl#transformation_3"
    scoped["inputs"][0]["id"] = f"{scope}/input-data"
    scoped["outputs"][0]["id"] = f"{scope}/merged"
    scoped["outputs"][0]["outputSource"] = f"{scope}/step0/merged"
    step = scoped["steps"][0]
    step["id"] = f"{scope}/step0"
    step["in"] = [{"id": f"{scope}/step0/input-data", "source": f"{scope}/input-data"}]
    step["out"] = [{"id": f"{scope}/step0/merged"}]
    assert root_id(scoped) == root_id(short)


def test_a_relative_reference_keeps_a_step_named_like_its_workflow():
    def named(process_id: str) -> dict:
        wf = workflow(merge_tool())
        wf["id"] = process_id
        wf["steps"][0]["id"] = "s0"
        wf["outputs"][0]["outputSource"] = "s0/merged"
        return wf

    root, nodes = decompose(named("s0"))
    assert json.loads(nodes[root].body)["outputs"]["merged"]["outputSource"] == (
        "s0/merged"
    )
    assert root == root_id(named("something-else"))


def test_anonymous_schema_names_do_not_change_the_identity():
    a, b = merge_tool(), merge_tool()
    for tool, name in ((a, "_:16606bec"), (b, "_:ad4e1bb5")):
        tool["inputs"]["input-data"]["type"] = {
            "type": "array",
            "items": "File",
            "name": name,
        }
    assert root_id(a) == root_id(b)


def test_what_runs_changes_the_identity():
    assert root_id(merge_tool("hadd")) != root_id(merge_tool("hadd -f"))
    renamed = merge_tool()
    renamed["inputs"] = {"files": renamed["inputs"]["input-data"]}
    assert root_id(renamed) != root_id(merge_tool())


def test_default_values_are_kept_verbatim():
    a, b = merge_tool(), merge_tool()
    a["inputs"]["conf"] = {"type": "Any", "default": {"label": "A"}}
    b["inputs"]["conf"] = {"type": "Any", "default": {"label": "B"}}
    assert root_id(a) != root_id(b)


@pytest.mark.parametrize(
    "where",
    [
        lambda t, v: t.update(
            requirements={"EnvVarRequirement": {"envDef": {"label": v, "doc": v}}}
        ),
        lambda t, v: t["inputs"].update(
            seed={"type": {"type": "record", "fields": {"label": v, "doc": "int"}}}
        ),
        lambda t, v: t.update(hints={"SoftwareRequirement": {"packages": {"doc": v}}}),
    ],
)
def test_user_chosen_names_called_doc_or_label_are_kept(where):
    a, b = merge_tool(), merge_tool()
    where(a, "string")
    where(b, "File")
    assert root_id(a) != root_id(b)


def typed(name: str, reference: str, value_type: str = "int") -> dict:
    tool = merge_tool()
    tool["requirements"] = {
        "SchemaDefRequirement": {
            "types": [
                {"name": "#Colour", "type": "enum", "symbols": ["#Colour/red"]},
                {
                    "name": name,
                    "type": "record",
                    "fields": [{"name": "value", "type": value_type}],
                },
            ]
        }
    }
    tool["inputs"]["seed"] = {"type": reference}
    tool["inputs"]["seeds"] = {"type": f"{reference}[]"}
    return tool


def test_named_types_are_written_by_short_name():
    root = root_id(typed("Seed", "Seed"))
    assert root == root_id(typed("#Seed", "#Seed"))
    assert root == root_id(typed("file:///p/merge.cwl#main/Seed", "#main/Seed"))
    assert root != root_id(typed("Seed", "Seed", value_type="string"))
    body = json.loads(decompose(typed("#main/Seed", "#main/Seed"))[1][root].body)
    assert body["inputs"]["seeds"]["type"] == {"type": "array", "items": "Seed"}
    assert body["requirements"]["SchemaDefRequirement"]["types"][0]["symbols"] == [
        "red"
    ]


def test_named_types_must_be_defined_by_the_process_using_them():
    tool = merge_tool()
    tool["inputs"]["seed"] = {"type": "Seed"}
    with pytest.raises(ValueError, match="not defined by the process"):
        decompose(workflow(tool))


def test_pack_scopes_named_types_by_component():
    root, nodes = decompose(workflow(typed("Seed", "Seed")))
    packed = pack(root, nodes)
    tool = packed["$graph"][1]
    schema = tool["requirements"]["SchemaDefRequirement"]["types"]
    assert schema[1]["name"] == tool["id"] + "/Seed"
    assert tool["inputs"]["seeds"]["type"]["items"] == tool["id"] + "/Seed"
    assert unpack(packed) == (root, nodes)


def test_child_change_propagates_to_parents_but_not_siblings():
    before_root, before = decompose(workflow(merge_tool("a"), merge_tool("b")))
    after_root, after = decompose(workflow(merge_tool("a"), merge_tool("c")))
    assert before_root != after_root
    assert root_id(merge_tool("a")) in before and root_id(merge_tool("a")) in after


def test_shared_child_is_one_node():
    root, nodes = decompose(workflow(merge_tool(), merge_tool()))
    assert len(nodes) == 2
    assert nodes[root].children == (root_id(merge_tool()),)


def test_child_body_is_a_reference_to_its_identity():
    root, nodes = decompose(workflow(merge_tool()))
    body = json.loads(nodes[root].body)
    child = parse_reference(body["steps"]["step0"]["run"])
    assert child == root_id(merge_tool())
    assert body["outputs"]["merged"]["outputSource"] == "step0/merged"


def test_existing_reference_is_kept_without_its_body():
    child = root_id(merge_tool())
    wf = workflow(merge_tool())
    wf["steps"][0]["run"] = format_reference(child)
    assert root_id(wf) == root_id(workflow(merge_tool()))


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda p: p.update(cwlVersion="v1.0"), "upgrade"),
        (lambda p: p["steps"][0].update(run="tools/merge.cwl"), "Unresolved"),
        (lambda p: p.update({"class": "Nope"}), "Not a CWL process"),
        (
            lambda p: p.update(
                requirements={
                    "InitialWorkDirRequirement": {
                        "listing": [{"entryname": "f", "entry": {"$include": "x"}}]
                    }
                }
            ),
            r"Unresolved \$include",
        ),
        (
            lambda p: p["steps"][0]["run"]["inputs"].update(
                conf={"type": "Any", "default": {"$import": "conf.yml"}}
            ),
            r"Unresolved \$import",
        ),
        (lambda p: p.update({"$base": "http://example.org/"}), r"Unresolved \$base"),
    ],
)
def test_rejected_documents(mutate, message):
    wf = workflow(merge_tool())
    mutate(wf)
    with pytest.raises(ValueError, match=message):
        decompose(wf)


def test_pack_round_trips_to_the_same_tree():
    root, nodes = decompose(workflow(merge_tool("a"), merge_tool("b")))
    packed = pack(root, nodes)
    assert packed["$graph"][0]["id"] == "#main"
    assert unpack(packed) == (root, nodes)


def test_unpack_rejects_a_tampered_component():
    root, nodes = decompose(workflow(merge_tool()))
    packed = pack(root, nodes)
    packed["$graph"][1]["baseCommand"] = ["rm", "-rf"]
    with pytest.raises(ValueError, match="does not match"):
        unpack(packed)


def test_unpack_verifies_a_shared_component_once():
    # Each level runs the level below twice: 2**40 paths, 41 components
    nodes = decompose(merge_tool())[1]
    root = next(iter(nodes))
    for _ in range(40):
        level = workflow(merge_tool(), merge_tool())
        for step in level["steps"]:
            step["run"] = format_reference(root)
        root, own = decompose(level)
        nodes.update(own)
    assert unpack(pack(root, nodes)) == (root, nodes)


def test_unpack_rejects_a_component_that_runs_itself():
    packed = pack(*decompose(workflow(merge_tool())))
    packed["$graph"][0]["steps"]["step0"]["run"] = "#main"
    with pytest.raises(ValueError, match="runs itself"):
        unpack(packed)


def test_source_lists_and_scatter_are_shortened_like_single_sources():
    def scattered(scope: str) -> dict:
        wf = workflow(merge_tool())
        wf["requirements"] = {
            "ScatterFeatureRequirement": {},
            "MultipleInputFeatureRequirement": {},
        }
        wf["inputs"].append({"id": f"{scope}extra", "type": "File[]"})
        step = wf["steps"][0]
        step["in"] = {
            "input-data": {
                "source": [f"{scope}input-data", f"{scope}extra"],
                "linkMerge": "merge_flattened",
            }
        }
        step["scatter"] = "input-data"
        step["dirac:Group"] = {"size": 4}
        return wf

    root, nodes = decompose(scattered("#transformation_3/"))
    step = json.loads(nodes[root].body)["steps"]["step0"]
    assert step["in"]["input-data"]["source"] == ["input-data", "extra"]
    assert step["scatter"] == "input-data"
    assert "dirac:Group" not in step
    assert root == root_id(scattered(""))


@pytest.mark.parametrize(
    "short, expanded",
    [
        ("File[]", {"type": "array", "items": "File"}),
        ("File?", ["null", "File"]),
        ("File[]?", ["null", {"type": "array", "items": "File"}]),
        ("File[][]", {"type": "array", "items": {"type": "array", "items": "File"}}),
        (["File?", "int"], ["null", "File", "int"]),
    ],
)
def test_type_shorthands_are_expanded(short, expanded):
    a, b = merge_tool(), merge_tool()
    a["inputs"]["input-data"]["type"] = short
    b["inputs"]["input-data"]["type"] = expanded
    root, nodes = decompose(a)
    assert root == root_id(b)
    body = json.loads(nodes[root].body)
    assert body["inputs"]["input-data"]["type"] == expanded


def test_union_types_name_their_members():
    a, b = typed("Seed", "Seed"), typed("Seed", "Seed")
    b["inputs"]["seed"] = {"type": ["null", "#main/Seed"]}
    a["inputs"]["seed"] = {"type": ["null", "Seed"]}
    root, nodes = decompose(a)
    assert root == root_id(b)
    assert json.loads(nodes[root].body)["inputs"]["seed"]["type"] == ["null", "Seed"]


def test_a_process_id_without_a_fragment_is_ignored():
    a = merge_tool()
    a["id"] = "file:///home/someone/prod/merge.cwl"
    assert root_id(a) == root_id(merge_tool())


@pytest.mark.parametrize(
    "reference, message",
    [
        ("tools/merge.cwl", "Not a process reference"),
        ("proc:sha256-abcd", "Not a SHA-256 process reference"),
    ],
)
def test_malformed_process_references_are_refused(reference, message):
    with pytest.raises(ValueError, match=message):
        parse_reference(reference)


def test_duplicate_ids_are_refused():
    tool = merge_tool()
    tool["inputs"] = [{"id": "x", "type": "File"}, {"id": "#main/x", "type": "File"}]
    tool["id"] = "#main"
    with pytest.raises(ValueError, match="Duplicate id 'x'"):
        decompose(tool)


def test_canonical_bytes_are_compact_and_sorted():
    assert canonical_bytes({"b": 1, "a": "é"}) == '{"a":"é","b":1}'.encode()


def test_golden_identity():
    # Changing this value means every stored identity changes: bump
    # CANONICAL_VERSION and plan the migration instead of editing it.
    assert (
        root_id(workflow(merge_tool())).hex()
        == "b06bc1e02806b735f5100eff9d5e9b00fb0ea154c0856d3e03b5d500689344a9"
    )
