from __future__ import annotations

import subprocess

import pytest
from schema_salad.fetcher import DefaultFetcher

from diracx.core.cwl import decompose, pack
from diracx.logic.transformation.validation import (
    InvalidProcessError,
    _only,
    validate_packed,
)


def workflow(source_type: str, output: dict | None = None) -> dict:
    tool = {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": ["hadd"],
        "inputs": {"files": {"type": "File[]", "inputBinding": {"position": 1}}},
        "outputs": output or {"out": {"type": "File", "outputBinding": {"glob": "*"}}},
    }
    return {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "inputs": {"files": source_type},
        "outputs": {"out": {"type": "File", "outputSource": "s0/out"}},
        "steps": {"s0": {"run": tool, "in": {"files": "files"}, "out": ["out"]}},
    }


def packed(process: dict) -> dict:
    return pack(*decompose(process))


def test_valid_process_passes():
    validate_packed(packed(workflow("File[]")))


def test_schema_errors_are_reported_without_the_temporary_path():
    with pytest.raises(InvalidProcessError) as e:
        validate_packed(
            packed(
                workflow("File[]", output={"out": {"type": "File", "outputBinding": 3}})
            )
        )
    assert "process.cwl" not in str(e.value)


def test_unknown_fields_are_refused():
    process = workflow("File[]")
    process["steps"]["s0"]["run"]["nonsense"] = 1
    with pytest.raises(InvalidProcessError, match="nonsense"):
        validate_packed(packed(process))


def test_validation_reads_nothing_but_the_document(tmp_path, monkeypatch):
    secret = tmp_path / "secret"
    secret.write_text("do not read")
    fetched = []
    original = DefaultFetcher.fetch_text

    def spy(self, url, content_types=None):
        fetched.append(url)
        return original(self, url, content_types)

    monkeypatch.setattr(DefaultFetcher, "fetch_text", spy)
    document = packed(workflow("File[]"))
    document["$graph"][1]["requirements"] = [
        {
            "class": "InitialWorkDirRequirement",
            "listing": [{"entryname": "f", "entry": {"$include": secret.as_uri()}}],
        }
    ]
    with pytest.raises(InvalidProcessError):
        validate_packed(document)
    assert fetched and all(url.endswith("/process.cwl") for url in fetched)


def test_expanded_formats_still_validate():
    process = workflow("File[]")
    process["$namespaces"] = {"edam": "http://edamontology.org/"}
    process["inputs"]["files"] = {"type": "File[]", "format": "edam:format_2572"}
    document = packed(process)
    assert "http://edamontology.org/format_2572" in str(document)
    validate_packed(document)


def test_named_types_survive_packing():
    def seed(value_type: str) -> dict:
        return {
            "SchemaDefRequirement": {
                "types": [
                    {"name": "Seed", "type": "record", "fields": {"value": value_type}}
                ]
            }
        }

    process = workflow("File[]")
    process["requirements"] = seed("string")
    process["inputs"]["seed"] = "Seed"
    tool = process["steps"]["s0"]["run"]
    tool["requirements"] = seed("int")
    tool["inputs"]["seed"] = "Seed?"
    validate_packed(packed(process))


def test_the_fetcher_sees_no_other_file(tmp_path):
    allowed, other = tmp_path / "process.cwl", tmp_path / "other.cwl"
    allowed.write_text("{}")
    other.write_text("{}")
    fetcher = _only(allowed.as_uri())()
    assert fetcher.check_exists(allowed.as_uri() + "#main")
    assert not fetcher.check_exists(other.as_uri())


def test_validation_starts_no_process(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError(f"started a process: {args[0]!r}")

    monkeypatch.setattr(subprocess, "Popen", refuse)
    process = workflow("File[]")
    tool = process["steps"]["s0"]["run"]
    tool["requirements"] = {"InlineJavascriptRequirement": {}}
    tool["arguments"] = ["$(inputs.files.length + 1)"]
    validate_packed(packed(process))


def test_a_docker_image_validates_without_being_pulled(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError(f"started a process: {args[0]!r}")

    monkeypatch.setattr(subprocess, "Popen", refuse)
    process = workflow("File[]")
    process["steps"]["s0"]["run"]["requirements"] = {
        "DockerRequirement": {"dockerPull": "harbor.cta-observatory.org/ctapipe:0.24"}
    }
    validate_packed(packed(process))
