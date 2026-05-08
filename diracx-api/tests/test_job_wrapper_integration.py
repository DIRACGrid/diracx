"""Integration tests for JobWrapper: full pre_process -> run_job -> post_process chain.

This test exercises the complete JobWrapper lifecycle with:
- A CWL workflow file on disk (read by JobWrapper and dirac-cwl-runner)
- Mocked external services (DataManager, sandbox download/upload, JobReport, client)
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ruamel.yaml import YAML

import diracx.api.job_wrapper as _jw_mod
from diracx.api.job_wrapper import JobWrapper


def _build_workflow_dict() -> dict:
    """Return a minimal CWL CommandLineTool as a plain dict.

    The tool runs ``cat <input_file>`` and captures stdout. The dirac:Job
    hint declares the input_data / input_sandbox / output_sandbox sources.
    """
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": ["cat"],
        "label": "integration-test",
        "hints": [
            {
                "class": "dirac:Job",
                "schema_version": "1.0",
                "type": "User",
                "input_data": [{"source": "input_file"}],
                "input_sandbox": [{"source": "helper_script"}],
                "output_sandbox": [{"source": "stdout_log"}],
            }
        ],
        "inputs": [
            {"id": "input_file", "type": "File", "inputBinding": {"position": 1}},
            {"id": "helper_script", "type": "File"},
        ],
        "outputs": [{"id": "stdout_log", "type": "stdout"}],
        "stdout": "stdout.log",
    }


def _write_job_files(out_dir: Path, params: dict) -> tuple[Path, Path]:
    """Dump workflow + params to *out_dir* as YAML, return their paths."""
    workflow_path = out_dir / "workflow.cwl"
    params_path = out_dir / "params.yaml"
    yaml = YAML()
    with open(workflow_path, "w") as f:
        yaml.dump(_build_workflow_dict(), f)
    with open(params_path, "w") as f:
        yaml.dump(params, f)
    return workflow_path, params_path


# Default input parameters used by all three tests
_DEFAULT_PARAMS: dict = {
    "input_file": {"class": "File", "location": "LFN:/test/data/input.txt"},
    "helper_script": {
        "class": "File",
        "location": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh",
    },
}


class _FakeFifoReader:
    """No-op stand-in for PrmonFifoReader (skips FIFO open/blocking thread)."""

    def __init__(self, fifo_path, **_kw):
        self.latest_row = None
        self.compressed_series: list = []

    async def run(self) -> None:  # noqa: D401
        return

    def write_compressed(self, _output_path) -> None:
        return


def _install_dirac_dm_mock(dm_mock: MagicMock) -> None:
    """Stub the DIRAC DataManager module that JobWrapper imports dynamically."""
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


class _AbsPathWrapper(JobWrapper):
    """Resolve the relative ``Path(".") / "workernode" / <rand>`` to absolute.

    pre_process needs an absolute job path because parts of it call
    ``Path.as_uri()`` for the replica map, which rejects relative paths.
    """

    async def pre_process(self, params, job_hint):
        self._job_path = self._job_path.resolve()
        return await super().pre_process(params, job_hint)


@pytest.fixture()
def job_files(tmp_path):
    """Create test data files and return their paths."""
    data_file = tmp_path / "input.txt"
    data_file.write_text("Hello from LFN data file\n")

    helper_script = tmp_path / "helper.sh"
    helper_script.write_text("#!/bin/bash\necho 'helper executed'\n")
    helper_script.chmod(0o755)

    return {"data_file": data_file, "helper_script": helper_script, "tmp": tmp_path}


def _build_dm_mock(data_file: Path, lfn: str) -> MagicMock:
    """Build a MagicMock DataManager pre-loaded with replica/file metadata."""
    dm_mock = MagicMock()
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
    return dm_mock


class TestJobWrapperIntegration:
    """Full integration test: run_job with mocked external services."""

    async def test_run_job_full_chain(self, job_files, monkeypatch, tmp_path):
        """Verify run_job() succeeds through the full chain.

        Exercises hint parsing, sandbox download, LFN download, replica map
        building, SB injection, real CWL execution, and output parsing.
        """
        data_file: Path = job_files["data_file"]
        helper_script: Path = job_files["helper_script"]

        workflow_path, params_path = _write_job_files(tmp_path, _DEFAULT_PARAMS)

        # download_sandbox copies the helper script into the job dir
        sandbox_calls: list[tuple[str, Path]] = []

        async def mock_download_sandbox(sb_ref: str, job_path: Path) -> None:
            sandbox_calls.append((sb_ref, job_path))
            shutil.copy(helper_script, job_path / "helper.sh")

        monkeypatch.setattr(_jw_mod, "download_sandbox", mock_download_sandbox)

        async def mock_create_sandbox(_files) -> str:
            return "SB:SandboxSE|/S3/store/sha256:output.tar.zst"

        monkeypatch.setattr(_jw_mod, "create_sandbox", mock_create_sandbox)

        lfn = "/test/data/input.txt"
        _install_dirac_dm_mock(_build_dm_mock(data_file, lfn))

        job_report_mock = MagicMock()
        job_report_mock.set_job_status = MagicMock()
        job_report_mock.commit = AsyncMock()
        job_report_mock.send_heartbeat = AsyncMock(return_value=[])

        diracx_client_mock = MagicMock()
        diracx_client_mock.jobs.assign_sandbox_to_job = AsyncMock()

        monkeypatch.chdir(tmp_path)
        job_path = tmp_path / "workernode" / "1234"
        job_path.mkdir(parents=True)

        # Preserve job_path for inspection after the test
        rmtree_calls: list[Path] = []

        def mock_rmtree(path, **_kw):
            rmtree_calls.append(Path(path))

        # prmon is not installed in the test env -- strip it from the cmd line
        # so the real subprocess execs dirac-cwl-runner directly.
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
            patch.object(_jw_mod.os, "mkfifo", lambda _p: None),
            patch.object(_jw_mod, "PrmonFifoReader", _FakeFifoReader),
            patch.object(
                _jw_mod.asyncio,
                "create_subprocess_exec",
                side_effect=_strip_prmon,
            ),
            patch("random.randint", return_value=1234),
        ):
            wrapper = _AbsPathWrapper(job_id=42)
            result = await wrapper.run_job(workflow_path, params_path)

        assert result is True, "run_job should return True on success"

        assert len(sandbox_calls) == 1
        sb_ref_called, _ = sandbox_calls[0]
        assert sb_ref_called == "SB:SandboxSE|/S3/store/sha256:abc.tar.zst"

        # DataManager.getFile must have been called with the LFN
        # (DataManager mock retrieved via the dynamically-installed module)
        dm_class = sys.modules[
            "DIRAC.DataManagementSystem.Client.DataManager"
        ].DataManager
        dm_instance = dm_class.return_value
        dm_instance.getFile.assert_called_once()
        assert lfn in dm_instance.getFile.call_args[0][0]

        assert job_report_mock.set_job_status.call_count > 0
        assert job_report_mock.commit.call_count > 0
        assert len(rmtree_calls) == 1

        # job_path is preserved (rmtree was mocked) -- inspect contents
        assert job_path.exists(), "job_path should still exist (rmtree mocked)"
        assert (job_path / "parameter.yaml").exists(), "parameter.yaml must be written"

        # replica_map.json must contain both the LFN and the SB: entries
        replica_map_path = job_path / "replica_map.json"
        assert replica_map_path.exists(), "replica_map.json must be created"

        replica_map = json.loads(replica_map_path.read_text())
        assert lfn in replica_map, f"replica_map must contain LFN {lfn}"
        sb_key = "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#helper.sh"
        assert sb_key in replica_map, f"replica_map must contain SB key {sb_key}"

        # cwltool's stdout output file must exist in the job dir
        assert (job_path / "stdout.log").exists(), "stdout.log output file must exist"

        # cwltool lifecycle stderr lines must reach JobReport as ApplicationStatus
        app_status_calls = [
            call
            for call in job_report_mock.set_job_status.call_args_list
            if call.kwargs.get("application_status") is not None
            and call.args == ()
            and set(call.kwargs.keys()) == {"application_status"}
        ]
        assert len(app_status_calls) > 0, (
            "cwltool lifecycle lines should be relayed as ApplicationStatus"
        )
        for call in app_status_calls:
            status = call.kwargs["application_status"]
            assert status.startswith("["), (
                f"ApplicationStatus should start with '[', got: {status!r}"
            )

    async def test_stderr_lines_stored_as_application_status(
        self, job_files, monkeypatch, tmp_path
    ):
        """Each cwltool stderr lifecycle line should land in ApplicationStatus.

        Uses a fake subprocess to emit known stderr lines; verifies
        set_job_status is called with each lifecycle line and commit() runs.
        """
        data_file: Path = job_files["data_file"]
        helper_script: Path = job_files["helper_script"]

        workflow_path, params_path = _write_job_files(tmp_path, _DEFAULT_PARAMS)

        async def mock_download_sandbox(_sb_ref, job_path):
            shutil.copy(helper_script, job_path / "helper.sh")

        monkeypatch.setattr(_jw_mod, "download_sandbox", mock_download_sandbox)

        async def mock_create_sandbox(_files):
            return "SB:SandboxSE|/S3/store/sha256:output.tar.zst"

        monkeypatch.setattr(_jw_mod, "create_sandbox", mock_create_sandbox)

        lfn = "/test/data/input.txt"
        _install_dirac_dm_mock(_build_dm_mock(data_file, lfn))

        job_report_mock = MagicMock()
        job_report_mock.set_job_status = MagicMock()
        job_report_mock.commit = AsyncMock()
        job_report_mock.send_heartbeat = AsyncMock(return_value=[])

        diracx_client_mock = MagicMock()
        diracx_client_mock.jobs.assign_sandbox_to_job = AsyncMock()

        monkeypatch.chdir(tmp_path)
        job_path = tmp_path / "workernode" / "1234"
        job_path.mkdir(parents=True)

        # Mix lifecycle lines (matched by _CWLTOOL_STATUS_RE) with noise lines.
        known_stderr = [
            "INFO Resolved '/tmp/task.cwl' to 'file:///tmp/task.cwl'",
            "INFO [job echo_job] /tmp/xyz$ echo hello",
            "INFO [job echo_job] completed success",
            "INFO Final process status is success",
        ]
        # Only lifecycle lines become ApplicationStatus, with log prefix stripped
        expected_statuses = ["[job echo_job] completed success"]

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

        async def mock_create_subprocess(*_args, **kwargs):
            cwd = kwargs.get("cwd")
            if cwd:
                (Path(cwd) / "stdout.log").write_text("Hello from LFN data file\n")
            return fake_proc

        with (
            patch.object(_jw_mod, "AsyncDiracClient", return_value=diracx_client_mock),
            patch.object(_jw_mod, "JobReport", return_value=job_report_mock),
            patch.object(_jw_mod.shutil, "rmtree", lambda p, **kw: None),
            patch.object(_jw_mod.shutil, "which", return_value="/usr/bin/prmon"),
            patch.object(_jw_mod.os, "mkfifo", lambda _p: None),
            patch.object(_jw_mod, "PrmonFifoReader", _FakeFifoReader),
            patch("random.randint", return_value=1234),
            patch.object(
                _jw_mod.asyncio,
                "create_subprocess_exec",
                side_effect=mock_create_subprocess,
            ),
        ):
            wrapper = _AbsPathWrapper(job_id=42)
            result = await wrapper.run_job(workflow_path, params_path)

        assert result is True, "run_job should return True with mocked Popen"

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

        # commit() runs in pre_process, after stderr loop, and in post_process.
        assert job_report_mock.commit.call_count >= 1

    async def test_run_job_starts_monitor(self, job_files, monkeypatch, tmp_path):
        """run_job should start a JobMonitor that runs alongside the subprocess."""
        data_file: Path = job_files["data_file"]
        helper_script: Path = job_files["helper_script"]

        workflow_path, params_path = _write_job_files(tmp_path, _DEFAULT_PARAMS)

        async def mock_download_sandbox(_sb_ref, job_path):
            shutil.copy(helper_script, job_path / "helper.sh")

        monkeypatch.setattr(_jw_mod, "download_sandbox", mock_download_sandbox)

        async def mock_create_sandbox(_files):
            return "SB:SandboxSE|/S3/store/sha256:output.tar.zst"

        monkeypatch.setattr(_jw_mod, "create_sandbox", mock_create_sandbox)

        lfn = "/test/data/input.txt"
        _install_dirac_dm_mock(_build_dm_mock(data_file, lfn))

        job_report_mock = MagicMock()
        job_report_mock.set_job_status = MagicMock()
        job_report_mock.commit = AsyncMock()
        job_report_mock.send_heartbeat = AsyncMock(return_value=[])

        diracx_client_mock = MagicMock()
        diracx_client_mock.jobs.assign_sandbox_to_job = AsyncMock()

        monkeypatch.chdir(tmp_path)
        job_path = tmp_path / "workernode" / "1234"
        job_path.mkdir(parents=True)

        # Track whether JobMonitor was started
        monitor_started: list[bool] = []
        import diracx.api.job_monitor as _jm_mod

        class _TrackingMonitor(_jm_mod.JobMonitor):
            async def run(self):
                monitor_started.append(True)
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    pass

        monkeypatch.setattr(_jw_mod, "JobMonitor", _TrackingMonitor)

        # Fake subprocess so run_job completes without prmon / dirac-cwl-runner
        known_stderr = ["INFO [job cat_job] completed success"]

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

        async def mock_create_subprocess(*_args, **kwargs):
            cwd = kwargs.get("cwd")
            if cwd:
                (Path(cwd) / "stdout.log").write_text("Hello from LFN data file\n")
            return fake_proc

        with (
            patch.object(_jw_mod, "AsyncDiracClient", return_value=diracx_client_mock),
            patch.object(_jw_mod, "JobReport", return_value=job_report_mock),
            patch.object(_jw_mod.shutil, "rmtree", lambda p, **kw: None),
            patch.object(_jw_mod.shutil, "which", return_value="/usr/bin/prmon"),
            patch.object(_jw_mod.os, "mkfifo", lambda _p: None),
            patch.object(_jw_mod, "PrmonFifoReader", _FakeFifoReader),
            patch("random.randint", return_value=1234),
            patch.object(
                _jw_mod.asyncio,
                "create_subprocess_exec",
                side_effect=mock_create_subprocess,
            ),
        ):
            wrapper = _AbsPathWrapper(job_id=42)
            result = await wrapper.run_job(workflow_path, params_path)

        assert result is True
        assert len(monitor_started) == 1, "JobMonitor should have been started"
