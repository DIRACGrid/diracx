"""Tests for CWL submission logic: models, parsing, and cwl_to_jdl translation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from diracx.core.models.cwl import IOSource, JobHint, OutputDataEntry
from diracx.logic.jobs.cwl_submission import (
    build_matcher_docs,
    compute_workflow_id,
    cwl_to_jdl,
    extract_job_hint,
    parse_cwl,
    validate_requirements,
)

# --- Model tests ---


def test_job_hint_defaults():
    hint = JobHint(schema_version="1.0")
    assert hint.priority == 5
    assert hint.type == "User"
    assert hint.log_level == "INFO"
    assert hint.matcher == []
    assert hint.legacy_jdl == {}
    assert hint.input_sandbox == []
    assert hint.output_data == []


def test_job_hint_full():
    hint = JobHint(
        schema_version="1.0",
        priority=3,
        matcher=[
            {"site": "LCG.CERN.cern", "cpu": {"architecture": {"name": "x86_64"}}},
            {"site": "LCG.RAL.uk", "tags": "cvmfs:lhcb"},
        ],
        legacy_jdl={"CPUTime": 864000},
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
    assert len(hint.matcher) == 2
    assert hint.matcher[0]["site"] == "LCG.CERN.cern"
    assert hint.legacy_jdl == {"CPUTime": 864000}
    assert len(hint.output_data) == 1
    assert hint.output_data[0].output_se == ["SE-TAPE"]


def test_job_hint_default_schema_version():
    hint = JobHint()
    assert hint.schema_version == "1.0"


def test_job_hint_rejects_invalid_schema_version():
    with pytest.raises(ValidationError):
        JobHint(schema_version="99.0")


def test_io_source():
    ref = IOSource(source="config_files")
    assert ref.source == "config_files"


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
    matcher:
      - site: "LCG.CERN.cern"
    legacy_jdl:
      CPUTime: 100000

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
    assert len(hint.matcher) == 1
    assert hint.matcher[0]["site"] == "LCG.CERN.cern"


def test_extract_job_hint_missing():
    task = parse_cwl(CWL_NO_HINT)
    with pytest.raises(ValueError, match="missing required dirac:Job hint"):
        extract_job_hint(task)


def test_unsupported_schema_version():
    cwl = MINIMAL_CWL.replace('schema_version: "1.0"', 'schema_version: "99.0"')
    task = parse_cwl(cwl)
    with pytest.raises(ValidationError):
        extract_job_hint(task)


# --- cwl_to_jdl tests ---


def test_cwl_to_jdl_basic():
    task = parse_cwl(MINIMAL_CWL)
    hint = extract_job_hint(task)
    matcher_docs = build_matcher_docs(task, hint)
    jdl = cwl_to_jdl(task, hint, matcher_docs, None)

    assert 'Executable = "dirac-cwl-exec"' in jdl
    assert 'JobType = "User"' in jdl
    assert "Priority = 3" in jdl
    assert 'JobName = "test-job"' in jdl
    assert '"LCG.CERN.cern"' in jdl
    # legacy_jdl CPUTime should be merged in
    assert "CPUTime = 100000" in jdl


def test_cwl_to_jdl_legacy_jdl_override():
    """legacy_jdl fields are merged into the JDL output."""
    task = parse_cwl(MINIMAL_CWL)
    hint = extract_job_hint(task)
    matcher_docs = build_matcher_docs(task, hint)
    jdl = cwl_to_jdl(task, hint, matcher_docs, None)

    assert "CPUTime = 100000" in jdl


def test_cwl_to_jdl_output_sandbox():
    task = parse_cwl(CWL_WITH_IO)
    hint = extract_job_hint(task)
    matcher_docs = build_matcher_docs(task, hint)
    jdl = cwl_to_jdl(task, hint, matcher_docs, None)

    assert '"std.err"' in jdl


def test_cwl_to_jdl_output_data():
    task = parse_cwl(CWL_WITH_IO)
    hint = extract_job_hint(task)
    matcher_docs = build_matcher_docs(task, hint)
    jdl = cwl_to_jdl(task, hint, matcher_docs, None)

    assert '"result.root"' in jdl
    assert "OutputPath" in jdl
    assert "SE-USER" in jdl


def test_cwl_to_jdl_input_data_with_params():
    task = parse_cwl(CWL_WITH_IO)
    hint = extract_job_hint(task)
    matcher_docs = build_matcher_docs(task, hint)
    params = {
        "input_lfns": [
            {"class": "File", "path": "LFN:/lhcb/data/file1.root"},
            {"class": "File", "path": "LFN:/lhcb/data/file2.root"},
        ],
        "helper_script": {"class": "File", "path": "helper.sh"},
    }
    jdl = cwl_to_jdl(task, hint, matcher_docs, params)

    assert "LFN:/lhcb/data/file1.root" in jdl
    assert "LFN:/lhcb/data/file2.root" in jdl
    assert "helper.sh" in jdl


def test_cwl_to_jdl_bad_source_reference():
    """A source reference to a non-existent CWL input should fail."""
    cwl = CWL_WITH_IO.replace("source: helper_script", "source: nonexistent_input")
    task = parse_cwl(cwl)
    hint = extract_job_hint(task)
    matcher_docs = build_matcher_docs(task, hint)
    with pytest.raises(ValueError, match="nonexistent_input"):
        cwl_to_jdl(task, hint, matcher_docs, None)


class TestRangeExpansion:
    """Tests for server-side range expansion."""

    def test_basic_range(self):
        from diracx.logic.jobs.cwl_submission import expand_range_inputs

        result = expand_range_inputs(
            range_param="seed",
            range_start=0,
            range_end=5,
            range_step=1,
            base_inputs={"message": "hello"},
        )
        assert len(result) == 5
        assert result[0] == {"message": "hello", "seed": 0}
        assert result[4] == {"message": "hello", "seed": 4}

    def test_range_with_step(self):
        from diracx.logic.jobs.cwl_submission import expand_range_inputs

        result = expand_range_inputs(
            range_param="seed",
            range_start=0,
            range_end=10,
            range_step=2,
            base_inputs=None,
        )
        assert len(result) == 5
        assert [r["seed"] for r in result] == [0, 2, 4, 6, 8]

    def test_range_no_base_inputs(self):
        from diracx.logic.jobs.cwl_submission import expand_range_inputs

        result = expand_range_inputs(
            range_param="idx",
            range_start=0,
            range_end=3,
            range_step=1,
            base_inputs=None,
        )
        assert result == [{"idx": 0}, {"idx": 1}, {"idx": 2}]


# --- Requirement whitelist tests ---

CWL_WITH_INLINE_JS = """\
cwlVersion: v1.2
class: CommandLineTool
label: js-tool
baseCommand: echo

requirements:
  - class: InlineJavascriptRequirement

hints:
  - class: dirac:Job
    schema_version: "1.0"

inputs:
  - id: msg
    type: string
outputs: []

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
"""

CWL_WITH_DOCKER = """\
cwlVersion: v1.2
class: CommandLineTool
label: docker-tool
baseCommand: echo

requirements:
  - class: DockerRequirement
    dockerPull: ubuntu:22.04

hints:
  - class: dirac:Job
    schema_version: "1.0"

inputs: []
outputs: []

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
"""

CWL_WITH_MPI = """\
cwlVersion: v1.2
class: CommandLineTool
label: mpi-tool
baseCommand: echo

requirements:
  - class: MPIRequirement
    processes: 4

hints:
  - class: dirac:Job
    schema_version: "1.0"

inputs: []
outputs: []

$namespaces:
  dirac: "https://diracgrid.org/cwl#"
  cwltool: "http://commonwl.org/cwltool#"
"""


def test_validate_requirements_pass_through():
    """Pass-through requirements should be accepted."""
    task = parse_cwl(CWL_WITH_INLINE_JS)
    validate_requirements(task)  # Should not raise


def test_validate_requirements_rejects_docker():
    """DockerRequirement should be rejected."""
    task = parse_cwl(CWL_WITH_DOCKER)
    with pytest.raises(ValueError, match="DockerRequirement"):
        validate_requirements(task)


def test_validate_requirements_rejects_mpi():
    """MPIRequirement should be rejected."""
    task = parse_cwl(CWL_WITH_MPI)
    with pytest.raises(ValueError, match="MPIRequirement"):
        validate_requirements(task)


def test_validate_requirements_no_requirements():
    """A CWL with no requirements should pass validation."""
    task = parse_cwl(CWL_NO_HINT)
    validate_requirements(task)  # Should not raise


class TestBuildMatcherDocs:
    """Tests for building matcher docs from hint + CWL requirements."""

    def test_no_matcher_no_requirements(self):
        """No matcher key and no supported requirements -> [{}]."""
        hint = JobHint()
        task = parse_cwl(CWL_NO_HINT)
        docs = build_matcher_docs(task, hint)
        assert docs == [{}]

    def test_matcher_passed_through(self):
        """Matcher docs from hint are preserved."""
        hint = JobHint(
            matcher=[
                {"site": "SiteA", "tags": "cvmfs:lhcb"},
                {"site": "SiteB"},
            ]
        )
        task = parse_cwl(CWL_NO_HINT)
        docs = build_matcher_docs(task, hint)
        assert len(docs) == 2
        assert docs[0] == {"site": "SiteA", "tags": "cvmfs:lhcb"}
        assert docs[1] == {"site": "SiteB"}

    def test_empty_matcher_list(self):
        """Empty matcher list -> [{}]."""
        hint = JobHint(matcher=[])
        task = parse_cwl(CWL_NO_HINT)
        docs = build_matcher_docs(task, hint)
        assert docs == [{}]
