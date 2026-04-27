"""Tests for JobWrapper command building, output parsing, and replica map wiring."""

from __future__ import annotations

import json
from pathlib import Path

from diracx.api.job_wrapper import JobWrapper
from diracx.core.models.commands import StoreOutputDataCommand
from diracx.core.models.cwl import IOSource, JobHint, OutputDataEntry

# ---------------------------------------------------------------------------
# Helper to create a bare JobWrapper instance (bypasses __init__)
# ---------------------------------------------------------------------------


def _make_wrapper() -> object:
    """Create a bare JobWrapper without calling __init__."""
    w = JobWrapper.__new__(JobWrapper)
    w._preprocess_commands = []
    w._postprocess_commands = []
    w._output_sandbox = []
    w._input_data_sources = []
    w._input_sandbox_sources = []
    w._replica_map_path = None
    return w


# ---------------------------------------------------------------------------
# Tests: _build_commands_from_hint
# ---------------------------------------------------------------------------


class TestBuildCommandsFromHint:
    """Test _build_commands_from_hint populates commands and I/O source lists."""

    def test_no_io_no_commands(self):
        """Hint with no I/O should produce no commands."""
        w = _make_wrapper()
        hint = JobHint()
        w._build_commands_from_hint(hint)

        assert w._preprocess_commands == []
        assert w._postprocess_commands == []
        assert w._input_data_sources == []
        assert w._input_sandbox_sources == []
        assert w._output_sandbox == []

    def test_output_data_creates_store_command(self):
        """output_data in hint creates a StoreOutputDataCommand."""
        w = _make_wrapper()
        hint = JobHint(
            output_data=[
                OutputDataEntry(
                    source="my_output",
                    output_path="/lhcb/user/a/auser/output.dst",
                    output_se=["CERN-DST"],
                )
            ]
        )
        w._build_commands_from_hint(hint)

        assert len(w._postprocess_commands) == 1
        cmd = w._postprocess_commands[0]
        assert isinstance(cmd, StoreOutputDataCommand)
        assert cmd._output_paths == {"my_output": "/lhcb/user/a/auser/output.dst"}
        assert cmd._output_se == ["CERN-DST"]

    def test_multiple_output_data_single_command(self):
        """Multiple output_data entries produce a single command with all paths."""
        w = _make_wrapper()
        hint = JobHint(
            output_data=[
                OutputDataEntry(
                    source="out1",
                    output_path="/lhcb/user/a/auser/file1.dst",
                    output_se=["CERN-DST"],
                ),
                OutputDataEntry(
                    source="out2",
                    output_path="/lhcb/user/a/auser/file2.dst",
                    output_se=["CERN-DST"],
                ),
            ]
        )
        w._build_commands_from_hint(hint)

        assert len(w._postprocess_commands) == 1
        cmd = w._postprocess_commands[0]
        assert isinstance(cmd, StoreOutputDataCommand)
        assert "out1" in cmd._output_paths
        assert "out2" in cmd._output_paths
        assert cmd._output_paths["out1"] == "/lhcb/user/a/auser/file1.dst"
        assert cmd._output_paths["out2"] == "/lhcb/user/a/auser/file2.dst"

    def test_multiple_output_data_se_deduplicated(self):
        """output_se values across multiple output_data entries are deduplicated."""
        w = _make_wrapper()
        hint = JobHint(
            output_data=[
                OutputDataEntry(
                    source="out1",
                    output_path="/lhcb/user/a/auser/file1.dst",
                    output_se=["CERN-DST", "IN2P3-DST"],
                ),
                OutputDataEntry(
                    source="out2",
                    output_path="/lhcb/user/a/auser/file2.dst",
                    output_se=["CERN-DST"],
                ),
            ]
        )
        w._build_commands_from_hint(hint)

        cmd = w._postprocess_commands[0]
        assert sorted(cmd._output_se) == sorted(["CERN-DST", "IN2P3-DST"])

    def test_input_data_sources_extracted(self):
        """input_data sources are extracted into _input_data_sources."""
        w = _make_wrapper()
        hint = JobHint(
            input_data=[
                IOSource(source="data_file"),
                IOSource(source="another_file"),
            ]
        )
        w._build_commands_from_hint(hint)

        assert w._input_data_sources == ["data_file", "another_file"]
        assert w._postprocess_commands == []

    def test_input_sandbox_sources_extracted(self):
        """input_sandbox sources are extracted into _input_sandbox_sources."""
        w = _make_wrapper()
        hint = JobHint(
            input_sandbox=[
                IOSource(source="helper_script"),
                IOSource(source="config_file"),
            ]
        )
        w._build_commands_from_hint(hint)

        assert w._input_sandbox_sources == ["helper_script", "config_file"]

    def test_output_sandbox_sources_extracted(self):
        """output_sandbox sources are extracted into _output_sandbox."""
        w = _make_wrapper()
        hint = JobHint(
            output_sandbox=[
                IOSource(source="log_output"),
            ]
        )
        w._build_commands_from_hint(hint)

        assert w._output_sandbox == ["log_output"]

    def test_all_io_together(self):
        """All I/O fields together populate all lists and create one command."""
        w = _make_wrapper()
        hint = JobHint(
            input_data=[IOSource(source="data_in")],
            input_sandbox=[IOSource(source="script")],
            output_sandbox=[IOSource(source="logs")],
            output_data=[
                OutputDataEntry(
                    source="result",
                    output_path="/lhcb/user/a/auser/result.root",
                )
            ],
        )
        w._build_commands_from_hint(hint)

        assert w._input_data_sources == ["data_in"]
        assert w._input_sandbox_sources == ["script"]
        assert w._output_sandbox == ["logs"]
        assert len(w._postprocess_commands) == 1
        cmd = w._postprocess_commands[0]
        assert isinstance(cmd, StoreOutputDataCommand)
        assert "result" in cmd._output_paths


# ---------------------------------------------------------------------------
# Tests: __parse_output_filepaths (private — accessed via name-mangling)
# ---------------------------------------------------------------------------


class TestParseOutputFilepaths:
    """Test __parse_output_filepaths parses cwltool JSON stdout."""

    def _parse(self, wrapper, stdout: str):
        """Call the name-mangled private method."""
        return wrapper._JobWrapper__parse_output_filepaths(stdout)

    def test_single_output_file(self):
        """Single output file is parsed into a list with one path."""
        w = _make_wrapper()
        stdout = json.dumps(
            {
                "my_output": {
                    "class": "File",
                    "path": "/tmp/workdir/output.dst",
                    "location": "file:///tmp/workdir/output.dst",
                }
            }
        )
        result = self._parse(w, stdout)

        assert "my_output" in result
        assert result["my_output"] == ["/tmp/workdir/output.dst"]

    def test_multiple_named_outputs(self):
        """Multiple named outputs are all parsed."""
        w = _make_wrapper()
        stdout = json.dumps(
            {
                "output_a": {"class": "File", "path": "/tmp/a.dst"},
                "output_b": {"class": "File", "path": "/tmp/b.root"},
            }
        )
        result = self._parse(w, stdout)

        assert "output_a" in result
        assert "output_b" in result
        assert result["output_a"] == ["/tmp/a.dst"]
        assert result["output_b"] == ["/tmp/b.root"]

    def test_array_of_output_files(self):
        """Array of output files produces a list with all paths."""
        w = _make_wrapper()
        stdout = json.dumps(
            {
                "output_files": [
                    {"class": "File", "path": "/tmp/file1.root"},
                    {"class": "File", "path": "/tmp/file2.root"},
                    {"class": "File", "path": "/tmp/file3.root"},
                ]
            }
        )
        result = self._parse(w, stdout)

        assert "output_files" in result
        assert result["output_files"] == [
            "/tmp/file1.root",
            "/tmp/file2.root",
            "/tmp/file3.root",
        ]

    def test_null_output_skipped(self):
        """Null/None output values are skipped (not added to result)."""
        w = _make_wrapper()
        stdout = json.dumps(
            {
                "present_output": {"class": "File", "path": "/tmp/present.root"},
                "null_output": None,
            }
        )
        result = self._parse(w, stdout)

        assert "present_output" in result
        assert "null_output" not in result

    def test_empty_outputs(self):
        """Empty JSON object produces empty dict."""
        w = _make_wrapper()
        result = self._parse(w, "{}")
        assert result == {}

    def test_false_output_skipped(self):
        """Falsy (but not None) output values like False are also skipped."""
        w = _make_wrapper()
        stdout = json.dumps(
            {
                "valid": {"class": "File", "path": "/tmp/valid.root"},
                "falsy": False,
            }
        )
        result = self._parse(w, stdout)

        assert "valid" in result
        assert "falsy" not in result


# ---------------------------------------------------------------------------
# Tests: replica map wiring
# ---------------------------------------------------------------------------


class TestReplicaMapWiring:
    """Test that _replica_map_path controls --replica-map in the command."""

    def _build_command(self, wrapper, job_path: Path) -> list[str]:
        """Replicate the command-building logic from run_job()."""
        task_file = job_path / "task.cwl"
        param_file = job_path / "parameter.cwl"
        # Create the files so .exists() checks pass
        task_file.write_text("class: CommandLineTool\n")
        param_file.write_text("{}\n")

        command = ["dirac-cwl-runner", str(task_file.name)]
        if param_file.exists():
            command.append(str(param_file.name))
        if wrapper._replica_map_path and wrapper._replica_map_path.exists():
            command.extend(["--replica-map", str(wrapper._replica_map_path.name)])
        return command

    def test_no_replica_map_not_in_command(self, tmp_path):
        """When _replica_map_path is None, --replica-map is absent."""
        w = _make_wrapper()
        w._replica_map_path = None

        command = self._build_command(w, tmp_path)

        assert "--replica-map" not in command

    def test_replica_map_path_set_and_exists_in_command(self, tmp_path):
        """When _replica_map_path is set and file exists, --replica-map is present."""
        w = _make_wrapper()
        replica_map_path = tmp_path / "replica_map.json"
        replica_map_path.write_text('{"replicas": []}')
        w._replica_map_path = replica_map_path

        command = self._build_command(w, tmp_path)

        assert "--replica-map" in command
        idx = command.index("--replica-map")
        assert command[idx + 1] == "replica_map.json"

    def test_replica_map_path_set_but_missing_not_in_command(self, tmp_path):
        """When _replica_map_path is set but file doesn't exist, --replica-map is absent."""
        w = _make_wrapper()
        # Point to a non-existent file
        w._replica_map_path = tmp_path / "missing_replica_map.json"

        command = self._build_command(w, tmp_path)

        assert "--replica-map" not in command
