"""Integration tests for JobWrapper: full pre_process -> run_job -> post_process chain.

This test exercises the complete JobWrapper lifecycle with:
- A real CWL CommandLineTool (executed via dirac-cwl-runner)
- Mocked external services (DataManager, sandbox download/upload, JobReport, client)
- Real cwl_utils and ruamel.yaml
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Direct imports — all dependencies are available in the test environment
# ---------------------------------------------------------------------------
import cwl_utils.parser.cwl_v1_2 as _cwl_v1_2  # noqa: E402
import pytest

import diracx.api.job_wrapper as _jw_mod  # noqa: E402
from diracx.api.job_wrapper import JobWrapper  # noqa: E402

CommandLineTool = _cwl_v1_2.CommandLineTool


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
        stdout="stdout.log",
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
        # Build the CWL task and job model.
        # ------------------------------------------------------------------
        from diracx.core.models.cwl_submission import JobInputModel, JobModel

        task = _build_cwl_tool()

        job_input = JobInputModel(
            sandbox=None,
            cwl={
                "input_file": {
                    "class": "File",
                    "location": "LFN:/test/data/input.txt",
                },
                "helper_script": {
                    "class": "File",
                    "location": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh",
                },
            },
        )
        job_model = JobModel(task=task, input=job_input)

        # ------------------------------------------------------------------
        # Mock: download_sandbox — copies helper script into job_path
        # ------------------------------------------------------------------
        sandbox_calls: list[tuple[str, Path]] = []

        async def mock_download_sandbox(sb_ref: str, job_path: Path) -> None:
            sandbox_calls.append((sb_ref, job_path))
            dest = job_path / "helper.sh"
            shutil.copy(helper_script, dest)

        monkeypatch.setattr(_jw_mod, "download_sandbox", mock_download_sandbox)

        # ------------------------------------------------------------------
        # Mock: create_sandbox — returns a fake SB: reference
        # ------------------------------------------------------------------
        async def mock_create_sandbox(files) -> str:
            return "SB:SandboxSE|/S3/store/sha256:output.tar.zst"

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
        # Strip prmon prefix from the command so the real subprocess
        # runs dirac-cwl-runner directly (prmon is not installed locally)
        _real_create_subprocess = asyncio.create_subprocess_exec

        async def _strip_prmon(*args, **kwargs):
            args_list = list(args)
            if args_list and args_list[0] == "prmon" and "--" in args_list:
                sep = args_list.index("--")
                args_list = args_list[sep + 1 :]
            return await _real_create_subprocess(*args_list, **kwargs)

        with (
            patch.object(_jw_mod, "AsyncDiracClient", return_value=diracx_client_mock),
            patch.object(_jw_mod, "JobReport", return_value=job_report_mock),
            patch.object(_jw_mod.shutil, "rmtree", mock_rmtree),
            patch.object(_jw_mod.shutil, "which", return_value="/usr/bin/prmon"),
            patch.object(
                _jw_mod.asyncio,
                "create_subprocess_exec",
                side_effect=_strip_prmon,
            ),
            patch("random.randint", return_value=1234),
        ):
            wrapper = _AbsPathWrapper(job_id=42)
            result = await wrapper.run_job(job_model)

        # ------------------------------------------------------------------
        # Assertions
        # ------------------------------------------------------------------
        assert result is True, "run_job should return True on success"

        # Verify sandbox download was called with the right SB: reference
        assert len(sandbox_calls) == 1
        sb_ref_called, _ = sandbox_calls[0]
        assert sb_ref_called == "SB:SandboxSE|/S3/store/sha256:abc.tar.zst"

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
        assert (job_path / "parameter.yaml").exists(), "parameter.yaml must be written"

        # replica_map.json must exist with both LFN and SB: entries
        replica_map_path = job_path / "replica_map.json"
        assert replica_map_path.exists(), "replica_map.json must be created"

        replica_map = json.loads(replica_map_path.read_text())
        assert lfn in replica_map, f"replica_map must contain LFN {lfn}"
        sb_key = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert sb_key in replica_map, f"replica_map must contain SB key {sb_key}"

        # Output file (stdout.log) must exist in job dir
        assert (job_path / "stdout.log").exists(), "stdout.log output file must exist"

        # Verify stderr lines were streamed as ApplicationStatus
        app_status_calls = [
            call
            for call in job_report_mock.set_job_status.call_args_list
            if call.kwargs.get("application_status") is not None
            and call.args == ()
            and set(call.kwargs.keys()) == {"application_status"}
        ]
        # cwltool emits lifecycle lines; at least one should have been relayed
        assert len(app_status_calls) > 0, (
            "cwltool lifecycle lines should be relayed as ApplicationStatus"
        )
        # Verify no log level prefixes leak into ApplicationStatus
        for call in app_status_calls:
            status = call.kwargs["application_status"]
            assert status.startswith("["), (
                f"ApplicationStatus should start with '[', got: {status!r}"
            )

    async def test_stderr_lines_stored_as_application_status(
        self, job_files, monkeypatch, tmp_path
    ):
        """Verify that each stderr line from the subprocess is stored as ApplicationStatus.

        Uses a mocked Popen to emit known stderr lines and checks that
        set_job_status is called with each line as application_status, and
        commit() is called after each line.
        """
        data_file: Path = job_files["data_file"]
        helper_script: Path = job_files["helper_script"]

        from diracx.core.models.cwl_submission import JobInputModel, JobModel

        task = _build_cwl_tool()
        job_input = JobInputModel(
            sandbox=None,
            cwl={
                "input_file": {
                    "class": "File",
                    "location": "LFN:/test/data/input.txt",
                },
                "helper_script": {
                    "class": "File",
                    "location": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh",
                },
            },
        )
        job_model = JobModel(task=task, input=job_input)

        # Mock download_sandbox and create_sandbox
        async def mock_download_sandbox(sb_ref, job_path):
            shutil.copy(helper_script, job_path / "helper.sh")

        monkeypatch.setattr(_jw_mod, "download_sandbox", mock_download_sandbox)

        async def mock_create_sandbox(files):
            return "SB:SandboxSE|/S3/store/sha256:output.tar.zst"

        monkeypatch.setattr(_jw_mod, "create_sandbox", mock_create_sandbox)

        # Mock DataManager
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
                    lfn: {"Size": data_file.stat().st_size, "Checksum": "deadbeef"}
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

        # Mock JobReport and client
        job_report_mock = MagicMock()
        job_report_mock.set_job_status = MagicMock()
        job_report_mock.commit = AsyncMock()
        job_report_mock.send_heartbeat = AsyncMock(return_value=[])

        diracx_client_mock = MagicMock()
        diracx_client_mock.jobs.assign_sandbox_to_job = AsyncMock()

        monkeypatch.chdir(tmp_path)
        job_path = tmp_path / "workernode" / "1234"
        job_path.mkdir(parents=True)

        # Mock async subprocess to emit known stderr lines.
        # Mix lifecycle lines (matched by _STATUS_RE) with noise lines (not matched).
        known_stderr = [
            "INFO Resolved '/tmp/task.cwl' to 'file:///tmp/task.cwl'",
            "INFO [job echo_job] /tmp/xyz$ echo hello",
            "INFO [job echo_job] completed success",
            "INFO Final process status is success",
        ]
        # Only lifecycle lines become ApplicationStatus, with log prefix stripped
        expected_statuses = [
            "[job echo_job] completed success",
        ]

        class _FakeStderr:
            """Async iterator that yields encoded stderr lines."""

            def __init__(self, lines: list[bytes]):
                self._iter = iter(lines)

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return next(self._iter)
                except StopIteration:
                    raise StopAsyncIteration from None

        class _FakeStdout:
            """Async reader that returns encoded stdout."""

            def __init__(self, data: bytes):
                self._data = data

            async def read(self) -> bytes:
                return self._data

        fake_proc = MagicMock()
        fake_proc.pid = 99999
        fake_proc.stderr = _FakeStderr([f"{line}\n".encode() for line in known_stderr])
        fake_proc.stdout = _FakeStdout(
            b'{"stdout_log": {"class": "File", "path": "stdout.log"}}'
        )
        fake_proc.returncode = 0
        fake_proc.wait = AsyncMock(return_value=0)

        # Create the expected output file when subprocess is spawned
        async def mock_create_subprocess(*args, **kwargs):
            cwd = kwargs.get("cwd")
            if cwd:
                (Path(cwd) / "stdout.log").write_text("Hello from LFN data file\n")
            return fake_proc

        class _AbsPathWrapper(JobWrapper):
            async def pre_process(self, executable, arguments, job_hint):
                self._job_path = self._job_path.resolve()
                return await super().pre_process(executable, arguments, job_hint)

        with (
            patch.object(_jw_mod, "AsyncDiracClient", return_value=diracx_client_mock),
            patch.object(_jw_mod, "JobReport", return_value=job_report_mock),
            patch.object(_jw_mod.shutil, "rmtree", lambda p, **kw: None),
            patch.object(_jw_mod.shutil, "which", return_value="/usr/bin/prmon"),
            patch("random.randint", return_value=1234),
            patch.object(
                _jw_mod.asyncio,
                "create_subprocess_exec",
                side_effect=mock_create_subprocess,
            ),
        ):
            wrapper = _AbsPathWrapper(job_id=42)
            result = await wrapper.run_job(job_model)

        assert result is True, "run_job should return True with mocked Popen"

        # Extract application_status calls that came from the streaming loop
        # (these have only application_status kwarg and no positional args)
        streamed_statuses = [
            call.kwargs["application_status"]
            for call in job_report_mock.set_job_status.call_args_list
            if call.kwargs.get("application_status") is not None
            and call.args == ()
            and set(call.kwargs.keys()) == {"application_status"}
        ]
        assert streamed_statuses == expected_statuses, (
            f"Expected lifecycle lines {expected_statuses} as ApplicationStatus, "
            f"got {streamed_statuses}"
        )

        # commit() is called in pre_process, after the streaming loop (flush),
        # and in post_process — at least once for the flush
        assert job_report_mock.commit.call_count >= 1

    async def test_run_job_starts_monitor(self, job_files, monkeypatch, tmp_path):
        """Verify run_job starts a JobMonitor that runs alongside the subprocess."""
        import asyncio

        data_file: Path = job_files["data_file"]
        helper_script: Path = job_files["helper_script"]

        from diracx.core.models.cwl_submission import JobInputModel, JobModel

        task = _build_cwl_tool()
        job_input = JobInputModel(
            sandbox=None,
            cwl={
                "input_file": {"class": "File", "location": "LFN:/test/data/input.txt"},
                "helper_script": {
                    "class": "File",
                    "location": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh",
                },
            },
        )
        job_model = JobModel(task=task, input=job_input)

        async def mock_download_sandbox(sb_ref, job_path):
            shutil.copy(helper_script, job_path / "helper.sh")

        monkeypatch.setattr(_jw_mod, "download_sandbox", mock_download_sandbox)

        async def mock_create_sandbox(files):
            return "SB:SandboxSE|/S3/store/sha256:output.tar.zst"

        monkeypatch.setattr(_jw_mod, "create_sandbox", mock_create_sandbox)

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
                    lfn: {"Size": data_file.stat().st_size, "Checksum": "deadbeef"}
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

        dm_module = types.ModuleType("DIRAC.DataManagementSystem.Client.DataManager")
        dm_module.DataManager = MagicMock(return_value=dm_mock)
        sys.modules["DIRAC"] = types.ModuleType("DIRAC")
        sys.modules["DIRAC.DataManagementSystem"] = types.ModuleType(
            "DIRAC.DataManagementSystem"
        )
        sys.modules["DIRAC.DataManagementSystem.Client"] = types.ModuleType(
            "DIRAC.DataManagementSystem.Client"
        )
        sys.modules["DIRAC.DataManagementSystem.Client.DataManager"] = dm_module

        job_report_mock = MagicMock()
        job_report_mock.set_job_status = MagicMock()
        job_report_mock.commit = AsyncMock()
        job_report_mock.send_heartbeat = AsyncMock(return_value=[])

        diracx_client_mock = MagicMock()
        diracx_client_mock.jobs.assign_sandbox_to_job = AsyncMock()

        monkeypatch.chdir(tmp_path)
        job_path = tmp_path / "workernode" / "1234"
        job_path.mkdir(parents=True)

        class _AbsPathWrapper(JobWrapper):
            async def pre_process(self, executable, arguments, job_hint):
                self._job_path = self._job_path.resolve()
                return await super().pre_process(executable, arguments, job_hint)

        # Track whether JobMonitor was started
        monitor_started = []
        import diracx.api.job_monitor as _jm_mod

        class _TrackingMonitor(_jm_mod.JobMonitor):
            async def run(self):
                monitor_started.append(True)
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    pass

        # Patch in the job_wrapper module's namespace (direct import binding)
        monkeypatch.setattr(_jw_mod, "JobMonitor", _TrackingMonitor)

        # Mock subprocess so run_job completes without needing prmon or dirac-cwl-runner
        known_stderr = [
            "INFO [job cat_job] completed success",
        ]

        class _FakeStderr:
            def __init__(self, lines):
                self._iter = iter(lines)

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return next(self._iter)
                except StopIteration:
                    raise StopAsyncIteration from None

        class _FakeStdout:
            def __init__(self, data):
                self._data = data

            async def read(self):
                return self._data

        fake_proc = MagicMock()
        fake_proc.pid = 99999
        fake_proc.stderr = _FakeStderr([f"{line}\n".encode() for line in known_stderr])
        fake_proc.stdout = _FakeStdout(
            b'{"stdout_log": {"class": "File", "path": "stdout.log"}}'
        )
        fake_proc.returncode = 0
        fake_proc.wait = AsyncMock(return_value=0)

        async def mock_create_subprocess(*args, **kwargs):
            cwd = kwargs.get("cwd")
            if cwd:
                (Path(cwd) / "stdout.log").write_text("Hello from LFN data file\n")
            return fake_proc

        with (
            patch.object(_jw_mod, "AsyncDiracClient", return_value=diracx_client_mock),
            patch.object(_jw_mod, "JobReport", return_value=job_report_mock),
            patch.object(_jw_mod.shutil, "rmtree", lambda p, **kw: None),
            patch.object(_jw_mod.shutil, "which", return_value="/usr/bin/prmon"),
            patch("random.randint", return_value=1234),
            patch.object(
                _jw_mod.asyncio,
                "create_subprocess_exec",
                side_effect=mock_create_subprocess,
            ),
        ):
            wrapper = _AbsPathWrapper(job_id=42)
            result = await wrapper.run_job(job_model)

        assert result is True
        assert len(monitor_started) == 1, "JobMonitor should have been started"
