"""Integration tests for the dirac-cwl-run subprocess.

These tests run ``dirac-cwl-run`` as a real subprocess to prove the full
executor stack works end-to-end: mypyc patch → executor init → replica map
loading → LFN resolution → CWL execution → output files.

cwltool is required for these tests. If it is not installed the entire module
is skipped. Individual tests are also skipped if ``dirac-cwl-run`` is not on
PATH (e.g. in a virtualenv without the diracx-cli package installed).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

# Skip the whole module if cwltool is not available
cwltool = pytest.importorskip("cwltool", reason="cwltool not installed")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COPY_CWL = """\
cwlVersion: v1.2
class: CommandLineTool
baseCommand: [cp]

inputs:
  - id: input_file
    type: File
    inputBinding:
      position: 1

outputs:
  - id: output_file
    type: File
    outputBinding:
      glob: output.txt

arguments:
  - valueFrom: output.txt
    position: 2
"""


def _skip_if_no_dirac_cwl_run():
    if not shutil.which("dirac-cwl-run"):
        pytest.skip("dirac-cwl-run not found on PATH")


def _run(args: list[str], cwd: Path, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        cwd=cwd,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_basic_execution_with_replica_map(tmp_path):
    """Run a CWL cp-tool that resolves an LFN via a replica_map.json.

    Proves: mypyc patch → executor init → replica map loading → LFN
    resolution → CWL execution → output file produced.
    """
    _skip_if_no_dirac_cwl_run()

    # 1. Create a local input file with known content
    input_content = "hello from integration test\n"
    local_input = tmp_path / "local_input.txt"
    local_input.write_text(input_content)

    # 2. Write the CWL workflow
    cwl_file = tmp_path / "task.cwl"
    cwl_file.write_text(_COPY_CWL)

    # 3. Write the CWL inputs referencing an LFN
    lfn = "/test/data/input.txt"
    inputs_yaml = tmp_path / "inputs.yml"
    inputs_yaml.write_text(f"input_file:\n  class: File\n  location: 'LFN:{lfn}'\n")

    # 4. Write the replica_map.json mapping the LFN to the local file
    replica_map_data = {
        lfn: {
            "replicas": [
                {"url": local_input.as_uri(), "se": "local"},
            ],
            "size_bytes": len(input_content.encode()),
        }
    }
    replica_map_file = tmp_path / "replica_map.json"
    replica_map_file.write_text(json.dumps(replica_map_data))

    # 5. Create output directory
    outdir = tmp_path / "output"
    outdir.mkdir()

    # 6. Run dirac-cwl-run
    result = _run(
        [
            "dirac-cwl-run",
            str(cwl_file),
            str(inputs_yaml),
            "--outdir",
            str(outdir),
            "--replica-map",
            str(replica_map_file),
        ],
        cwd=tmp_path,
    )

    # Provide debug output on failure
    if result.returncode != 0:
        pytest.fail(
            f"dirac-cwl-run failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    # 7. Assert exit code 0
    assert result.returncode == 0, (
        f"Expected exit code 0, got {result.returncode}\nSTDERR: {result.stderr}"
    )

    # 8. Assert output file exists and content matches
    output_file = outdir / "output.txt"
    assert output_file.exists(), (
        f"Expected output file {output_file} to exist.\n"
        f"outdir contents: {list(outdir.iterdir())}"
    )
    assert output_file.read_text() == input_content


def test_execution_without_replica_map(tmp_path):
    """Run a CWL cp-tool using a plain local file (no LFN, no replica map).

    Proves that baseline CWL execution works through the dirac-cwl-run entry
    point even when no replica map is provided.
    """
    _skip_if_no_dirac_cwl_run()

    # 1. Create a local input file
    input_content = "baseline cwl execution test\n"
    local_input = tmp_path / "local_input.txt"
    local_input.write_text(input_content)

    # 2. Write the CWL workflow
    cwl_file = tmp_path / "task.cwl"
    cwl_file.write_text(_COPY_CWL)

    # 3. Write CWL inputs referencing the local file directly
    inputs_yaml = tmp_path / "inputs.yml"
    inputs_yaml.write_text(
        f"input_file:\n  class: File\n  location: '{local_input.as_uri()}'\n"
    )

    # 4. Create output directory
    outdir = tmp_path / "output"
    outdir.mkdir()

    # 5. Run dirac-cwl-run (no --replica-map)
    result = _run(
        [
            "dirac-cwl-run",
            str(cwl_file),
            str(inputs_yaml),
            "--outdir",
            str(outdir),
        ],
        cwd=tmp_path,
    )

    if result.returncode != 0:
        pytest.fail(
            f"dirac-cwl-run failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    assert result.returncode == 0

    output_file = outdir / "output.txt"
    assert output_file.exists(), (
        f"Expected output file {output_file} to exist.\n"
        f"outdir contents: {list(outdir.iterdir())}"
    )
    assert output_file.read_text() == input_content


def test_sb_reference_in_replica_map(tmp_path):
    """Run a CWL cp-tool where the input is resolved via an SB: reference.

    The SB: key in the replica map is mapped to a local file. Proves that
    sandbox references are handled correctly through the replica map.
    """
    _skip_if_no_dirac_cwl_run()

    # 1. Create the real local file the SB: entry will point to
    input_content = "sandbox file content for integration test\n"
    local_file = tmp_path / "extracted_helper.txt"
    local_file.write_text(input_content)

    # 2. Write the CWL workflow
    cwl_file = tmp_path / "task.cwl"
    cwl_file.write_text(_COPY_CWL)

    # 3. SB: key — use the full reference as it would appear in a job description
    sb_key = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.txt"

    # 4. Write CWL inputs with SB: location
    inputs_yaml = tmp_path / "inputs.yml"
    inputs_yaml.write_text(f"input_file:\n  class: File\n  location: '{sb_key}'\n")

    # 5. Write replica_map.json with the SB: key
    replica_map_data = {
        sb_key: {
            "replicas": [
                {"url": local_file.as_uri(), "se": "local"},
            ],
        }
    }
    replica_map_file = tmp_path / "replica_map.json"
    replica_map_file.write_text(json.dumps(replica_map_data))

    # 6. Create output directory
    outdir = tmp_path / "output"
    outdir.mkdir()

    # 7. Run dirac-cwl-run
    result = _run(
        [
            "dirac-cwl-run",
            str(cwl_file),
            str(inputs_yaml),
            "--outdir",
            str(outdir),
            "--replica-map",
            str(replica_map_file),
        ],
        cwd=tmp_path,
    )

    if result.returncode != 0:
        pytest.fail(
            f"dirac-cwl-run failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    assert result.returncode == 0

    output_file = outdir / "output.txt"
    assert output_file.exists(), (
        f"Expected output file {output_file} to exist.\n"
        f"outdir contents: {list(outdir.iterdir())}"
    )
    assert output_file.read_text() == input_content
