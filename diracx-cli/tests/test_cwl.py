"""Packing CWL files for submission.

``fixtures/mc-simulation`` holds the files of the MCSimulation workgraph of
#1080; diracx-core's ``mc-simulation.packed.json`` is them packed by cwltool.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from schema_salad.exceptions import ValidationException
from schema_salad.fetcher import DefaultFetcher

from diracx.cli.cwl import pack
from diracx.core.cwl import NotSelfContainedError, entry_identity

FIXTURES = Path(__file__).parent / "fixtures"
WORKGRAPH = FIXTURES / "mc-simulation" / "mc-simulation.cwl"
PACKED = (
    Path(__file__).parents[2]
    / "diracx-core"
    / "tests"
    / "fixtures"
    / "mc-simulation.packed.json"
)
TRANSFORMATIONS = ("#gauss.cwl", "#boole-moore.cwl", "#merge.cwl")


def write(directory: Path, name: str, text: str) -> Path:
    path = directory / name
    path.write_text(text)
    return path


def test_transformations_get_the_identities_cwltool_pack_gives_them():
    document = pack(WORKGRAPH).document
    packed = json.loads(PACKED.read_text())
    for entry_id in TRANSFORMATIONS:
        assert entry_identity(document, entry_id) == entry_identity(packed, entry_id)


def test_the_same_files_elsewhere_pack_the_same(tmp_path):
    shutil.copytree(WORKGRAPH.parent, tmp_path / "elsewhere")
    assert pack(tmp_path / "elsewhere" / WORKGRAPH.name) == pack(WORKGRAPH)


def test_dirac_hints_are_taken_out_as_written():
    submission = pack(WORKGRAPH)
    assert "diracgrid.org/cwl#" not in json.dumps(submission.document["$graph"])
    hints = submission.hints
    assert set(hints) == {
        "#main",
        "#main/events",
        "#main/MCSimulation",
        "#main/MCReconstruction",
        "#main/MCMerge",
    }
    assert hints["#main"]["Workgraph"]["type"] == "MCSimulation"
    assert hints["#main/events"]["Feeder"]["name"] == "Seeds"
    assert hints["#main/MCMerge"]["Transformation"]["packer"] == {
        "name": "ByGroupSizeRun",
        "args": {"group_size": 4, "keep_storage_together": True},
    }


def test_ids_and_names_inside_hints_are_kept_as_written(tmp_path):
    write(
        tmp_path,
        "tool.cwl",
        """
cwlVersion: v1.2
class: CommandLineTool
$namespaces: {dirac: https://diracgrid.org/cwl#}
hints:
  dirac:Workgraph:
    data_management:
    - id: Removal
      packer: {name: BySize}
baseCommand: [echo]
inputs: []
outputs: []
""",
    )
    hints = pack(tmp_path / "tool.cwl").hints
    assert hints["#main"]["Workgraph"]["data_management"] == [
        {"id": "Removal", "packer": {"name": "BySize"}}
    ]


def test_other_hints_stay(tmp_path):
    write(
        tmp_path,
        "tool.cwl",
        """
cwlVersion: v1.2
class: CommandLineTool
$namespaces: {dirac: https://diracgrid.org/cwl#}
hints:
  ResourceRequirement: {coresMin: 2}
  dirac:Transformation: {packer: {name: PerInput}}
baseCommand: [echo]
inputs: []
outputs: []
""",
    )
    submission = pack(tmp_path / "tool.cwl")
    assert submission.document["hints"] == [
        {"class": "ResourceRequirement", "coresMin": 2}
    ]


def test_captured_streams_get_fixed_names(tmp_path):
    text = """
cwlVersion: v1.2
class: CommandLineTool
baseCommand: [echo, hi]
inputs: []
outputs:
  out: stdout
  err: stderr
"""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a = pack(write(tmp_path / "a", "echo.cwl", text)).document
    b = pack(write(tmp_path / "b", "echo.cwl", text)).document
    assert a == b
    assert (a["stdout"], a["stderr"]) == ("cwl.stdout", "cwl.stderr")
    globs = {o["id"]: o["outputBinding"]["glob"] for o in a["outputs"]}
    assert globs == {"#main/out": "cwl.stdout", "#main/err": "cwl.stderr"}


def test_yaml_is_read_as_cwl_runners_read_it(tmp_path):
    write(
        tmp_path,
        "tool.cwl",
        """
cwlVersion: v1.2
class: CommandLineTool
baseCommand: [echo]
inputs:
  answer: {type: string, default: yes}
outputs: []
""",
    )
    assert pack(tmp_path / "tool.cwl").document["inputs"][0]["default"] == "yes"


def test_files_given_by_path_are_refused(tmp_path):
    write(tmp_path, "seeds.txt", "1\n")
    write(
        tmp_path,
        "tool.cwl",
        """
cwlVersion: v1.2
class: CommandLineTool
baseCommand: [cat]
inputs:
  seeds: {type: File, default: {class: File, path: seeds.txt}}
outputs: []
""",
    )
    with pytest.raises(NotSelfContainedError, match="names a path or location"):
        pack(tmp_path / "tool.cwl")


@pytest.mark.parametrize(
    "text",
    [
        """
cwlVersion: v1.2
class: Workflow
inputs: []
outputs: []
steps:
  s0: {run: "http://127.0.0.1:9/tool.cwl", in: [], out: []}
""",
        """
cwlVersion: v1.2
class: CommandLineTool
$schemas: ["http://127.0.0.1:9/schema.rdf"]
baseCommand: [echo]
inputs: []
outputs: []
""",
    ],
    ids=["run", "$schemas"],
)
def test_nothing_but_local_files_is_read(tmp_path, monkeypatch, text):
    fetched = []
    original = DefaultFetcher.fetch_text

    def spy(self, url, content_types=None):
        fetched.append(url)
        return original(self, url, content_types)

    monkeypatch.setattr(DefaultFetcher, "fetch_text", spy)
    with pytest.raises((ValidationException, NotSelfContainedError)):
        pack(write(tmp_path, "process.cwl", text))
    assert all(url.startswith("file://") for url in fetched)
