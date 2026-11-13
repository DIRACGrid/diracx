from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest
from schema_salad.fetcher import DefaultFetcher

from diracx.core.cwl import subgraph
from diracx.logic.transformation.validation import (
    InvalidProcessError,
    _only,
    validate_packed,
)

# The MCSimulation workgraph of #1080 as cwltool --pack writes it
FIXTURE = (
    Path(__file__).parents[3]
    / "diracx-core"
    / "tests"
    / "fixtures"
    / "mc-simulation.packed.json"
)


@pytest.fixture
def merge() -> dict:
    """The row MCMerge runs: #merge.cwl and the tool it runs."""
    return copy.deepcopy(subgraph(json.loads(FIXTURE.read_text()), "#merge.cwl"))


def entry(document: dict, entry_id: str) -> dict:
    return next(e for e in document["$graph"] if e["id"] == entry_id)


@pytest.mark.parametrize(
    "entry_id", ["#main", "#gauss.cwl", "#boole-moore.cwl", "#merge.cwl"]
)
def test_every_row_of_the_mc_simulation_workgraph_validates(entry_id):
    validate_packed(subgraph(json.loads(FIXTURE.read_text()), entry_id))


def test_errors_name_the_entry_not_the_temporary_file(merge):
    entry(merge, "#lb-run-app.cwl")["outputs"][0]["outputBinding"] = 3
    with pytest.raises(InvalidProcessError) as e:
        validate_packed(merge)
    assert "process.cwl" not in str(e.value)
    assert "`#lb-run-app.cwl/output-data`" in str(e.value)


def test_unknown_fields_are_refused(merge):
    entry(merge, "#merge.cwl")["nonsense"] = 1
    with pytest.raises(InvalidProcessError, match="`#merge.cwl`.*nonsense"):
        validate_packed(merge)


def test_validation_reads_nothing_but_the_document(merge, tmp_path, monkeypatch):
    secret = tmp_path / "secret"
    secret.write_text("do not read")
    fetched = []
    original = DefaultFetcher.fetch_text

    def spy(self, url, content_types=None):
        fetched.append(url)
        return original(self, url, content_types)

    monkeypatch.setattr(DefaultFetcher, "fetch_text", spy)
    entry(merge, "#lb-run-app.cwl")["requirements"][0]["listing"] = [
        {"entryname": "f", "entry": {"$include": secret.as_uri()}}
    ]
    with pytest.raises(InvalidProcessError):
        validate_packed(merge)
    assert fetched and all(url.endswith("/process.cwl") for url in fetched)


def test_expanded_formats_validate(merge):
    merge["$namespaces"] = {"edam": "http://edamontology.org/"}
    entry(merge, "#merge.cwl")["inputs"][0]["format"] = (
        "http://edamontology.org/format_2572"
    )
    validate_packed(merge)


def test_named_types_validate(merge):
    tool = entry(merge, "#lb-run-app.cwl")
    tool["requirements"].append(
        {
            "class": "SchemaDefRequirement",
            "types": [
                {
                    "name": "#lb-run-app.cwl/Seed",
                    "type": "record",
                    "fields": [{"name": "#lb-run-app.cwl/Seed/value", "type": "int"}],
                }
            ],
        }
    )
    tool["inputs"].append(
        {"id": "#lb-run-app.cwl/seed", "type": ["null", "#lb-run-app.cwl/Seed"]}
    )
    validate_packed(merge)


def test_the_fetcher_sees_no_other_file(tmp_path):
    allowed, other = tmp_path / "process.cwl", tmp_path / "other.cwl"
    allowed.write_text("{}")
    other.write_text("{}")
    fetcher = _only(allowed.as_uri())()
    assert fetcher.check_exists(allowed.as_uri() + "#main")
    assert not fetcher.check_exists(other.as_uri())


def test_validation_starts_no_process_nor_pulls_an_image(merge, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError(f"started a process: {args[0]!r}")

    monkeypatch.setattr(subprocess, "Popen", refuse)
    tool = entry(merge, "#lb-run-app.cwl")
    tool["arguments"].append("$(inputs['output-glob'].length + 1)")
    tool["requirements"].append(
        {
            "class": "DockerRequirement",
            "dockerPull": "harbor.cta-observatory.org/ctapipe:0.24",
        }
    )
    validate_packed(merge)
