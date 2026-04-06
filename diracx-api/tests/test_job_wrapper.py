"""Tests for JobWrapper command building, output parsing, and replica map wiring.

job_wrapper.py has heavy runtime dependencies (cwl_utils, DIRACCommon,
ruamel.yaml, diracx.client) that are not installed in the test environment.
We mock the unavailable modules at import time, then load job_wrapper.py
directly so we can test the pure-logic methods without those dependencies.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

from diracx.core.models.commands import StoreOutputDataCommand
from diracx.core.models.cwl import IOSource, JobHint, OutputDataEntry

# ---------------------------------------------------------------------------
# Mock unavailable modules so job_wrapper.py can be imported
# ---------------------------------------------------------------------------


def _ensure_mock(name: str) -> types.ModuleType:
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)
    return sys.modules[name]


_MOCK_MODULES = [
    "cwl_utils",
    "cwl_utils.parser",
    "cwl_utils.parser.cwl_v1_2",
    "DIRACCommon",
    "DIRACCommon.Core",
    "DIRACCommon.Core.Utilities",
    "DIRACCommon.Core.Utilities.ReturnValues",
    "ruamel",
    "ruamel.yaml",
    "diracx.api.job_report",
    "diracx.api.jobs",
    "diracx.client",
    "diracx.client.aio",
]

for _name in _MOCK_MODULES:
    _mod = _ensure_mock(_name)
    if _name == "cwl_utils.parser":
        _mod.save = None  # type: ignore[attr-defined]
    elif _name == "cwl_utils.parser.cwl_v1_2":
        for _attr in (
            "CommandLineTool",
            "ExpressionTool",
            "File",
            "Saveable",
            "Workflow",
        ):
            setattr(_mod, _attr, type(_attr, (), {}))
    elif _name == "DIRACCommon.Core.Utilities.ReturnValues":
        _mod.returnValueOrRaise = None  # type: ignore[attr-defined]
    elif _name == "ruamel.yaml":
        _mod.YAML = None  # type: ignore[attr-defined]
    elif _name == "diracx.api.job_report":
        _mod.JobReport = None  # type: ignore[attr-defined]
    elif _name == "diracx.api.jobs":
        _mod.create_sandbox = None  # type: ignore[attr-defined]
        _mod.download_sandbox = None  # type: ignore[attr-defined]
    elif _name == "diracx.client.aio":
        _mod.AsyncDiracClient = None  # type: ignore[attr-defined]


def _load_job_wrapper():
    """Load job_wrapper.py directly, bypassing package __init__ chains."""
    path = (
        Path(__file__).resolve().parent.parent
        / "src"
        / "diracx"
        / "api"
        / "job_wrapper.py"
    )
    spec = importlib.util.spec_from_file_location("diracx.api.job_wrapper", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_jw_mod = _load_job_wrapper()
JobWrapper = _jw_mod.JobWrapper


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

        command = ["dirac-cwl-run", str(task_file.name)]
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
