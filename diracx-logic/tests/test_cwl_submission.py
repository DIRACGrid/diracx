"""Tests for CWL submission logic: models, parsing, and cwl_to_jdl translation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from diracx.core.models.cwl import IOSource, JobHint, OutputDataEntry
from diracx.logic.jobs.cwl_submission import (
    compute_workflow_id,
    cwl_to_jdl,
    extract_job_hint,
    parse_cwl,
)

# --- Model tests ---


def test_job_hint_defaults():
    hint = JobHint(schema_version="1.0")
    assert hint.priority == 5
    assert hint.type == "User"
    assert hint.log_level == "INFO"
    assert hint.cpu_work is None
    assert hint.sites is None
    assert hint.input_sandbox == []
    assert hint.output_data == []


def test_job_hint_full():
    hint = JobHint(
        schema_version="1.0",
        priority=3,
        cpu_work=864000,
        platform="x86_64-el9",
        sites=["LCG.CERN.cern"],
        banned_sites=["LCG.RAL.uk"],
        tags=["GPU"],
        type="MCSimulation",
        group="lhcb_mc",
        input_data=[IOSource(source="input_lfns")],
        output_data=[
            OutputDataEntry(
                source="result",
                output_path="/lhcb/mc/output/",
                output_se=["SE-TAPE"],
            )
        ],
    )
    assert hint.cpu_work == 864000
    assert hint.sites == ["LCG.CERN.cern"]
    assert len(hint.output_data) == 1
    assert hint.output_data[0].output_se == ["SE-TAPE"]


def test_job_hint_requires_schema_version():
    with pytest.raises(ValidationError):
        JobHint()


def test_io_source_with_path():
    ref = IOSource(source="config_files", path="conf/")
    assert ref.source == "config_files"
    assert ref.path == "conf/"


def test_output_data_entry_default_se():
    entry = OutputDataEntry(source="result", output_path="/data/")
    assert entry.output_se == ["SE-USER"]


# --- Workflow ID tests ---


def test_compute_workflow_id_deterministic():
    cwl = "cwlVersion: v1.2\nclass: CommandLineTool\n"
    id1 = compute_workflow_id(cwl)
    id2 = compute_workflow_id(cwl)
    assert id1 == id2
    assert len(id1) == 64  # SHA-256 hex


def test_compute_workflow_id_different_content():
    id1 = compute_workflow_id("version1")
    id2 = compute_workflow_id("version2")
    assert id1 != id2


# --- CWL parsing and hint extraction ---

MINIMAL_CWL = """\
cwlVersion: v1.2
class: CommandLineTool
label: test-job
baseCommand: echo

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User
    priority: 3
    cpu_work: 100000
    sites:
      - LCG.CERN.cern
    banned_sites:
      - LCG.RAL.uk

requirements:
  - class: ResourceRequirement
    coresMin: 2
    coresMax: 4
    ramMin: 2048

inputs:
  - id: message
    type: string

outputs:
  - id: output_log
    type: File
    outputBinding:
      glob: "output.log"

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
"""

CWL_WITH_IO = """\
cwlVersion: v1.2
class: CommandLineTool
label: analysis-job
baseCommand: echo

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User
    input_sandbox:
      - source: helper_script
    input_data:
      - source: input_lfns
    output_sandbox:
      - source: stderr_log
    output_data:
      - source: result_file
        output_path: "/lhcb/user/output/"
        output_se:
          - SE-USER

inputs:
  - id: helper_script
    type: File
  - id: input_lfns
    type:
      type: array
      items: File
  - id: config_param
    type: string

outputs:
  - id: result_file
    type: File
    outputBinding:
      glob: "result.root"
  - id: stderr_log
    type: File
    outputBinding:
      glob: "std.err"

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
"""

CWL_NO_HINT = """\
cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo
inputs: []
outputs: []
"""


def test_parse_cwl():
    task = parse_cwl(MINIMAL_CWL)
    assert task.label == "test-job"


def test_extract_job_hint():
    task = parse_cwl(MINIMAL_CWL)
    hint = extract_job_hint(task)
    assert hint.schema_version == "1.0"
    assert hint.type == "User"
    assert hint.priority == 3
    assert hint.cpu_work == 100000


def test_extract_job_hint_missing():
    task = parse_cwl(CWL_NO_HINT)
    with pytest.raises(ValueError, match="missing required dirac:Job hint"):
        extract_job_hint(task)


def test_unsupported_schema_version():
    cwl = MINIMAL_CWL.replace('schema_version: "1.0"', 'schema_version: "99.0"')
    task = parse_cwl(cwl)
    with pytest.raises(ValueError, match="Unsupported dirac:Job schema_version"):
        extract_job_hint(task)


# --- cwl_to_jdl tests ---


def test_cwl_to_jdl_basic():
    task = parse_cwl(MINIMAL_CWL)
    hint = extract_job_hint(task)
    jdl = cwl_to_jdl(task, hint, None)

    assert 'Executable = "dirac-cwl-exec"' in jdl
    assert 'JobType = "User"' in jdl
    assert "Priority = 3" in jdl
    assert "CPUTime = 100000" in jdl
    assert 'JobName = "test-job"' in jdl
    assert "MinNumberOfProcessors = 2" in jdl
    assert "MaxNumberOfProcessors = 4" in jdl
    assert "MinRAM = 2048" in jdl
    assert '"LCG.CERN.cern"' in jdl
    assert '"LCG.RAL.uk"' in jdl


def test_cwl_to_jdl_multiprocessor_tags():
    task = parse_cwl(MINIMAL_CWL)
    hint = extract_job_hint(task)
    jdl = cwl_to_jdl(task, hint, None)

    assert '"MultiProcessor"' in jdl


def test_cwl_to_jdl_output_sandbox():
    task = parse_cwl(CWL_WITH_IO)
    hint = extract_job_hint(task)
    jdl = cwl_to_jdl(task, hint, None)

    assert '"std.err"' in jdl  # output sandbox from stderr_log


def test_cwl_to_jdl_output_data():
    task = parse_cwl(CWL_WITH_IO)
    hint = extract_job_hint(task)
    jdl = cwl_to_jdl(task, hint, None)

    assert '"result.root"' in jdl
    assert "OutputPath" in jdl
    assert "SE-USER" in jdl


def test_cwl_to_jdl_input_data_with_params():
    task = parse_cwl(CWL_WITH_IO)
    hint = extract_job_hint(task)
    params = {
        "input_lfns": [
            {"class": "File", "path": "LFN:/lhcb/data/file1.root"},
            {"class": "File", "path": "LFN:/lhcb/data/file2.root"},
        ],
        "helper_script": {"class": "File", "path": "helper.sh"},
    }
    jdl = cwl_to_jdl(task, hint, params)

    assert "LFN:/lhcb/data/file1.root" in jdl
    assert "LFN:/lhcb/data/file2.root" in jdl
    assert "helper.sh" in jdl


def test_cwl_to_jdl_bad_source_reference():
    """A source reference to a non-existent CWL input should fail."""
    cwl = CWL_WITH_IO.replace("source: helper_script", "source: nonexistent_input")
    task = parse_cwl(cwl)
    hint = extract_job_hint(task)
    with pytest.raises(ValueError, match="nonexistent_input"):
        cwl_to_jdl(task, hint, None)
