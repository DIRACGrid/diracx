"""Integration tests for JobWrapper: full pre_process -> run_job -> post_process chain.

This test exercises the complete JobWrapper lifecycle with:
- A real CWL CommandLineTool (executed via dirac-cwl-run)
- Mocked external services (DataManager, sandbox download/upload, JobReport, client)
- Real cwl_utils and ruamel.yaml (NOT mocked)

Requirements:
- ``dirac-cwl-run`` must be on PATH (skipped if not)
- ``cwltool`` must be importable (skipped if not)

Design note: conftest.py pre-loads and saves references to real cwl_utils and
ruamel.yaml objects before other test files can overwrite sys.modules entries.
This test uses those saved references to load job_wrapper with real types.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Skip guards
# ---------------------------------------------------------------------------

pytest.importorskip("cwltool", reason="cwltool not installed")

dirac_cwl_run = shutil.which("dirac-cwl-run")
pytestmark = pytest.mark.skipif(
    dirac_cwl_run is None,
    reason="dirac-cwl-run not on PATH",
)

# ---------------------------------------------------------------------------
# Import real module references saved by conftest.py before any mocking.
# conftest.py is always loaded before test modules, so these are guaranteed
# to be the real implementations. We access conftest via sys.modules since
# pytest installs it there automatically.
# ---------------------------------------------------------------------------

_conftest = sys.modules.get("conftest") or sys.modules.get("diracx-api/tests/conftest")
# Fallback: if conftest is not in sys.modules under 'conftest', search by path
if _conftest is None:
    for _key, _mod in list(sys.modules.items()):
        if hasattr(_mod, "_REAL_MODULES") and hasattr(_mod, "_REAL_CWL_PARSER"):
            _conftest = _mod
            break

assert _conftest is not None, (
    "conftest module not found in sys.modules — "
    "ensure conftest.py is in the tests directory"
)

_REAL_MODULES = _conftest._REAL_MODULES
_real_cwl_parser = _conftest._REAL_CWL_PARSER
_real_cwl_v1_2 = _conftest._REAL_CWL_V1_2
_real_ruamel_yaml = _conftest._REAL_RUAMEL_YAML

# The real CommandLineTool class (may differ from sys.modules version if mocked)
_RealCommandLineTool = _REAL_MODULES["cwl_utils.parser.cwl_v1_2.CommandLineTool"]

# ---------------------------------------------------------------------------
# Mock ONLY the modules that are unavailable or need stubbing.
# cwl_utils and ruamel.yaml are real — do NOT mock them.
# ---------------------------------------------------------------------------


def _ensure_mock(name: str) -> types.ModuleType:
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)
    return sys.modules[name]


_MOCK_ONLY = [
    "DIRACCommon",
    "DIRACCommon.Core",
    "DIRACCommon.Core.Utilities",
    "DIRACCommon.Core.Utilities.ReturnValues",
    "diracx.api.job_report",
    "diracx.api.jobs",
    "diracx.client",
    "diracx.client.aio",
]

for _name in _MOCK_ONLY:
    _mod = _ensure_mock(_name)

    if _name == "DIRACCommon.Core.Utilities.ReturnValues":
        # returnValueOrRaise must return result["Value"]
        def _return_value_or_raise(result):  # noqa: E301
            if not result.get("OK", False):
                raise RuntimeError(result.get("Message", "DIRAC error"))
            return result["Value"]

        _mod.returnValueOrRaise = _return_value_or_raise  # type: ignore[attr-defined]

    elif _name == "diracx.api.job_report":
        _mod.JobReport = MagicMock  # type: ignore[attr-defined]

    elif _name == "diracx.api.jobs":
        _mod.create_sandbox = None  # type: ignore[attr-defined]
        _mod.download_sandbox = None  # type: ignore[attr-defined]

    elif _name == "diracx.client.aio":
        _mod.AsyncDiracClient = MagicMock  # type: ignore[attr-defined]


def _load_job_wrapper() -> types.ModuleType:
    """Load job_wrapper.py with real cwl_utils/ruamel.yaml in sys.modules.

    Temporarily restores real module objects (saved by conftest.py) into
    sys.modules so that job_wrapper.py's imports bind real implementations.
    After loading, sys.modules entries are restored to whatever mocks are there.
    """
    # Save current (potentially mocked) state
    saved: dict[str, types.ModuleType | None] = {}
    real_names = ["cwl_utils.parser", "cwl_utils.parser.cwl_v1_2", "ruamel.yaml"]
    for name in real_names:
        saved[name] = sys.modules.get(name)

    # Restore real module objects and fix mocked attributes
    sys.modules["cwl_utils.parser"] = _real_cwl_parser
    # Restore .save on the parser module (test_job_wrapper.py sets it to None)
    _real_cwl_parser.save = _REAL_MODULES["cwl_utils.parser.save"]  # type: ignore[attr-defined]
    sys.modules["cwl_utils.parser.cwl_v1_2"] = _real_cwl_v1_2
    # Restore class attributes (test_job_wrapper.py overwrites them)
    for attr in ("CommandLineTool", "File", "Workflow", "ExpressionTool", "Saveable"):
        setattr(
            _real_cwl_v1_2, attr, _REAL_MODULES[f"cwl_utils.parser.cwl_v1_2.{attr}"]
        )
    sys.modules["ruamel.yaml"] = _real_ruamel_yaml
    # Restore YAML class (test_job_wrapper.py sets it to None)
    _real_ruamel_yaml.YAML = _REAL_MODULES["ruamel.yaml.YAML"]  # type: ignore[attr-defined]

    try:
        path = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "diracx"
            / "api"
            / "job_wrapper.py"
        )
        spec = importlib.util.spec_from_file_location(
            "diracx.api.job_wrapper_integ", path
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        # Restore whatever was in sys.modules before
        for name in real_names:
            if saved[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = saved[name]


_jw_mod = _load_job_wrapper()
JobWrapper = _jw_mod.JobWrapper


# ---------------------------------------------------------------------------
# CWL tool builder — uses the real CommandLineTool from conftest references
# ---------------------------------------------------------------------------

CommandLineTool = _RealCommandLineTool


def _build_cwl_tool() -> CommandLineTool:
    """Build a minimal CWL CommandLineTool for the integration test.

    The tool runs ``cat <input_file>`` and captures stdout.
    The dirac:Job hint declares:
      - input_data:   input_file  (an LFN)
      - input_sandbox: helper_script  (an SB: reference)
      - output_sandbox: stdout_log
    """
    return CommandLineTool(
        cwlVersion="v1.2",
        baseCommand=["cat"],
        label="integration-test",
        hints=[
            {
                "class": "dirac:Job",
                "schema_version": "1.0",
                "type": "User",
                "input_data": [{"source": "input_file"}],
                "input_sandbox": [{"source": "helper_script"}],
                "output_sandbox": [{"source": "stdout_log"}],
            }
        ],
        inputs=[
            {
                "id": "input_file",
                "type": "File",
                "inputBinding": {"position": 1},
            },
            {
                "id": "helper_script",
                "type": "File",
            },
        ],
        outputs=[
            {
                "id": "stdout_log",
                "type": "stdout",
            }
        ],
        stdout="std.out",
        id=None,
        requirements=None,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def job_files(tmp_path):
    """Create test data files and return their paths."""
    data_file = tmp_path / "input.txt"
    data_file.write_text("Hello from LFN data file\n")

    helper_script = tmp_path / "helper.sh"
    helper_script.write_text("#!/bin/bash\necho 'helper executed'\n")
    helper_script.chmod(0o755)

    return {
        "data_file": data_file,
        "helper_script": helper_script,
        "tmp": tmp_path,
    }


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------


class TestJobWrapperIntegration:
    """Full integration test: run_job with mocked external services."""

    async def test_run_job_full_chain(self, job_files, monkeypatch, tmp_path):
        """Verify run_job() succeeds through the full chain.

        Exercises hint parsing, sandbox download, LFN download, replica map
        building, SB injection, real CWL execution, and output parsing.
        """
        data_file: Path = job_files["data_file"]
        helper_script: Path = job_files["helper_script"]

        # ------------------------------------------------------------------
        # Build the CWL task.
        # JobModel Pydantic validation checks isinstance(task, CommandLineTool)
        # where CommandLineTool comes from cwl_submission's import. We must
        # ensure cwl_submission uses the same class as _build_cwl_tool().
        # Temporarily restore real cwl_utils so cwl_submission re-loads with
        # the real types, then create the model.
        # ------------------------------------------------------------------
        saved_parser = sys.modules.get("cwl_utils.parser")
        saved_v1_2 = sys.modules.get("cwl_utils.parser.cwl_v1_2")
        sys.modules["cwl_utils.parser"] = _real_cwl_parser
        _real_cwl_parser.save = _REAL_MODULES["cwl_utils.parser.save"]  # type: ignore[attr-defined]
        sys.modules["cwl_utils.parser.cwl_v1_2"] = _real_cwl_v1_2
        for _attr in (
            "CommandLineTool",
            "File",
            "Workflow",
            "ExpressionTool",
            "Saveable",
        ):
            setattr(
                _real_cwl_v1_2,
                _attr,
                _REAL_MODULES[f"cwl_utils.parser.cwl_v1_2.{_attr}"],
            )

        # Force cwl_submission to reload with real types
        sys.modules.pop("diracx.core.models.cwl_submission", None)

        try:
            from diracx.core.models.cwl_submission import JobInputModel, JobModel

            task = _build_cwl_tool()

            job_input = JobInputModel(
                sandbox=None,
                cwl={
                    "input_file": {
                        "class": "File",
                        "path": "LFN:/test/data/input.txt",
                    },
                    "helper_script": {
                        "class": "File",
                        "path": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh",
                    },
                },
            )
            job_model = JobModel(task=task, input=job_input)
        finally:
            # Restore mocked state
            if saved_parser is None:
                sys.modules.pop("cwl_utils.parser", None)
            else:
                sys.modules["cwl_utils.parser"] = saved_parser
            if saved_v1_2 is None:
                sys.modules.pop("cwl_utils.parser.cwl_v1_2", None)
            else:
                sys.modules["cwl_utils.parser.cwl_v1_2"] = saved_v1_2

        # ------------------------------------------------------------------
        # Mock: download_sandbox — copies helper script into job_path
        # ------------------------------------------------------------------
        sandbox_calls: list[tuple[str, Path]] = []

        async def mock_download_sandbox(pfn: str, job_path: Path) -> None:
            sandbox_calls.append((pfn, job_path))
            dest = job_path / "helper.sh"
            shutil.copy(helper_script, dest)

        monkeypatch.setattr(_jw_mod, "download_sandbox", mock_download_sandbox)

        # ------------------------------------------------------------------
        # Mock: create_sandbox — returns a fake SB path
        # ------------------------------------------------------------------
        async def mock_create_sandbox(files) -> str:
            return "SandboxSE|/S3/store/sha256:output.tar.zst"

        monkeypatch.setattr(_jw_mod, "create_sandbox", mock_create_sandbox)

        # ------------------------------------------------------------------
        # Mock: DIRAC.DataManagementSystem.Client.DataManager.DataManager
        # (imported dynamically inside __download_input_data)
        # ------------------------------------------------------------------
        dm_mock = MagicMock()
        lfn = "/test/data/input.txt"

        dm_mock.getActiveReplicas.return_value = {
            "OK": True,
            "Value": {
                "Successful": {lfn: {"LocalSE": f"file://{data_file}"}},
                "Failed": {},
            },
        }
        dm_mock.fileCatalog.getFileMetadata.return_value = {
            "OK": True,
            "Value": {
                "Successful": {
                    lfn: {
                        "Size": data_file.stat().st_size,
                        "Checksum": "deadbeef",
                    }
                },
                "Failed": {},
            },
        }

        def fake_get_file(lfns, dest_dir):
            successful = {}
            for _lfn in lfns:
                dest = Path(dest_dir).resolve() / data_file.name
                shutil.copy(data_file, dest)
                successful[_lfn] = str(dest)
            return {"OK": True, "Value": {"Successful": successful, "Failed": {}}}

        dm_mock.getFile.side_effect = fake_get_file

        # ------------------------------------------------------------------
        # Mock: JobReport and AsyncDiracClient
        # ------------------------------------------------------------------
        job_report_mock = MagicMock()
        job_report_mock.set_job_status = MagicMock()
        job_report_mock.commit = AsyncMock()

        diracx_client_mock = MagicMock()
        diracx_client_mock.jobs.assign_sandbox_to_job = AsyncMock()

        # ------------------------------------------------------------------
        # Redirect CWD to tmp_path so run_job's Path(".") resolves there.
        # ------------------------------------------------------------------
        monkeypatch.chdir(tmp_path)
        job_path = tmp_path / "workernode" / "1234"
        job_path.mkdir(parents=True)

        # Intercept shutil.rmtree to preserve the job dir for inspection
        rmtree_calls: list[Path] = []

        def mock_rmtree(path, **kwargs):
            rmtree_calls.append(Path(path))

        # ------------------------------------------------------------------
        # Install mock DataManager in sys.modules for the dynamic import
        # inside __download_input_data
        # ------------------------------------------------------------------
        dm_module = types.ModuleType("DIRAC.DataManagementSystem.Client.DataManager")
        dm_module.DataManager = MagicMock(return_value=dm_mock)  # type: ignore[attr-defined]
        sys.modules["DIRAC"] = types.ModuleType("DIRAC")
        sys.modules["DIRAC.DataManagementSystem"] = types.ModuleType(
            "DIRAC.DataManagementSystem"
        )
        sys.modules["DIRAC.DataManagementSystem.Client"] = types.ModuleType(
            "DIRAC.DataManagementSystem.Client"
        )
        sys.modules["DIRAC.DataManagementSystem.Client.DataManager"] = dm_module

        # ------------------------------------------------------------------
        # Subclass JobWrapper to resolve _job_path to absolute before
        # pre_process. run_job creates Path(".") / "workernode" / "<rand>",
        # which is relative; as_uri() requires an absolute path.
        # ------------------------------------------------------------------

        class _AbsPathWrapper(JobWrapper):
            """Subclass that resolves _job_path to absolute before pre_process."""

            async def pre_process(self, executable, arguments, job_hint):
                self._job_path = self._job_path.resolve()
                return await super().pre_process(executable, arguments, job_hint)

        # ------------------------------------------------------------------
        # Instantiate and run
        # ------------------------------------------------------------------
        with (
            patch.object(_jw_mod, "AsyncDiracClient", return_value=diracx_client_mock),
            patch.object(_jw_mod, "JobReport", return_value=job_report_mock),
            patch.object(_jw_mod.shutil, "rmtree", mock_rmtree),
            patch("random.randint", return_value=1234),
        ):
            wrapper = _AbsPathWrapper(job_id=42)
            result = await wrapper.run_job(job_model)

        # ------------------------------------------------------------------
        # Assertions
        # ------------------------------------------------------------------
        assert result is True, "run_job should return True on success"

        # Verify sandbox download was called with the right PFN
        assert len(sandbox_calls) == 1
        pfn_called, _ = sandbox_calls[0]
        assert pfn_called == "SandboxSE|/S3/store/sha256:abc.tar.zst"

        # Verify DataManager.getFile was called with the correct LFN
        dm_mock.getFile.assert_called_once()
        call_args = dm_mock.getFile.call_args
        assert lfn in call_args[0][0]

        # Verify JobReport methods were called
        assert job_report_mock.set_job_status.call_count > 0
        assert job_report_mock.commit.call_count > 0

        # Verify cleanup was triggered
        assert len(rmtree_calls) == 1

        # Inspect job directory (preserved since rmtree was mocked)
        assert job_path.exists(), "job_path should still exist (rmtree mocked)"
        assert (job_path / "task.cwl").exists(), "task.cwl must be written"
        assert (job_path / "parameter.cwl").exists(), "parameter.cwl must be written"

        # replica_map.json must exist with both LFN and SB: entries
        replica_map_path = job_path / "replica_map.json"
        assert replica_map_path.exists(), "replica_map.json must be created"

        replica_map = json.loads(replica_map_path.read_text())
        assert lfn in replica_map, f"replica_map must contain LFN {lfn}"
        sb_key = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert sb_key in replica_map, f"replica_map must contain SB key {sb_key}"

        # Output file (std.out) must exist in job dir
        assert (job_path / "std.out").exists(), "std.out output file must exist"
