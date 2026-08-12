# ruff: noqa: E402

"""Tests for the Condor job executor tasks."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

sys.modules.setdefault("htcondor2", types.ModuleType("htcondor2"))

from diracx.core.models import JobStatus
from diracx.tasks.jobs import CondorJobExecutorMonitorTask, CondorJobExecutorTask
from diracx.tasks.jobs import condor_job_executor as condor_job_executor_module
from diracx.tasks.plumbing.locks import MutexLock

FEATURE_ENABLED_ENV = "DIRACX_TASKS_CONDOR_JOB_EXECUTOR_ENABLED"
FEATURE_INTERVAL_ENV = "DIRACX_TASKS_CONDOR_JOB_EXECUTOR_INTERVAL_SECONDS"

SCHEDULER_STATE_SCRIPT = """
import json
import sys
import types
from datetime import UTC, datetime
from unittest.mock import MagicMock

sys.modules.setdefault("htcondor2", types.ModuleType("htcondor2"))

from diracx.tasks.plumbing.factory import load_task_registry
from diracx.tasks.plumbing.scheduler.scheduler import TaskScheduler

task_name = "jobs:CondorJobExecutorMonitorTask"
registry = load_task_registry()
task_cls = registry[task_name]
scheduler = TaskScheduler(
    broker=MagicMock(),
    redis_url="redis://unused",
    task_registry=registry,
)
before = datetime.now(tz=UTC)
scheduler._compute_initial_schedules()
next_run = scheduler._next_runs.get((task_name, ""))
print(
    json.dumps(
        {
            "enabled": task_cls._enabled,
            "tracked": next_run is not None,
            "delay_seconds": (
                (next_run - before).total_seconds() if next_run is not None else None
            ),
        }
    )
)
"""


def make_dependencies():
    return {
        "config": MagicMock(name="config"),
        "job_db": AsyncMock(name="job_db"),
        "job_logging_db": AsyncMock(name="job_logging_db"),
        "task_queue_db": MagicMock(name="task_queue_db"),
        "job_parameters_db": MagicMock(name="job_parameters_db"),
    }


def get_scheduler_state(feature_env: dict[str, str]) -> dict:
    env = os.environ.copy()
    env.pop(FEATURE_ENABLED_ENV, None)
    env.pop(FEATURE_INTERVAL_ENV, None)
    env.update(feature_env)
    result = subprocess.run(
        [sys.executable, "-c", SCHEDULER_STATE_SCRIPT],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )
    return json.loads(result.stdout)


def test_monitor_schedule_activation_is_environment_controlled():
    default_state = get_scheduler_state({})
    assert default_state == {
        "enabled": False,
        "tracked": False,
        "delay_seconds": None,
    }

    local_state = get_scheduler_state(
        {
            FEATURE_ENABLED_ENV: "true",
            FEATURE_INTERVAL_ENV: "10",
        }
    )
    assert local_state["enabled"] is True
    assert local_state["tracked"] is True
    assert 9 <= local_state["delay_seconds"] <= 11


def test_monitor_interval_must_be_positive():
    with pytest.raises(ValidationError):
        condor_job_executor_module.CondorJobExecutorSettings(interval_seconds=0)


def test_executor_takes_a_per_job_mutex():
    locks = CondorJobExecutorTask(job_id=42).execution_locks

    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert locks[0].redis_key == "lock:mutex:job:42"


async def test_executor_submits_job_and_marks_it_matched(monkeypatch):
    submission = condor_job_executor_module.CondorSubmitResult(
        cluster_id=1234,
        proc_id=7,
        schedd_name="analysis-schedd",
    )
    submit_to_condor = AsyncMock(return_value=submission)
    monkeypatch.setattr(CondorJobExecutorTask, "submit_to_condor", submit_to_condor)
    deps = make_dependencies()

    result = await CondorJobExecutorTask(job_id=42).execute(**deps)

    assert result == 42
    submit_to_condor.assert_awaited_once()
    deps["job_db"].set_job_attributes.assert_awaited_once()
    matched_updates = deps["job_db"].set_job_attributes.await_args.args[0]
    assert matched_updates[42]["Status"] == JobStatus.MATCHED
    assert matched_updates[42]["MinorStatus"] == "CondorExecutor"
    assert (
        matched_updates[42]["ApplicationStatus"]
        == "Submitted to HTCondor schedd analysis-schedd as 1234.7"
    )
    deps["job_logging_db"].insert_records.assert_awaited_once()


def test_jdl_key_value_pairs_are_extracted_as_dict():
    jdl = '[ Executable = "/bin/echo"; Arguments = "hello"; Requirements = (TARGET.FileSystemDomain == "disk"); ]'

    assert condor_job_executor_module._jdl_to_key_value_pairs(jdl) == {
        "Executable": "/bin/echo",
        "Arguments": "hello",
        "Requirements": 'TARGET.FileSystemDomain == "disk"',
    }


async def test_submit_to_condor_uses_htcondor_bindings(monkeypatch):
    class FakeDaemonTypes:
        Schedd = "Schedd"

    class FakeSubmitResult:
        def cluster(self):
            return 12345

    class FakeSchedd:
        def __init__(self, ad):
            self.ad = ad
            self.spool_called = False

        def submit(self, submit_obj, spool=False):
            assert "Executable = /bin/echo" in submit_obj.submit_text
            assert spool is True
            return FakeSubmitResult()

        def spool(self, submit_result):
            assert isinstance(submit_result, FakeSubmitResult)
            self.spool_called = True

    class FakeCollector:
        def __init__(self, host=None):
            self.host = host

        def locate(self, daemon_type, schedd_name):
            assert daemon_type == FakeDaemonTypes.Schedd
            return {"Name": schedd_name, "Collector": self.host}

    class FakeSubmit:
        def __init__(self, submit_text):
            self.submit_text = submit_text

    fake_htcondor = type(
        "FakeHtcondor",
        (),
        {
            "set_subsystem": staticmethod(lambda _: None),
            "param": {},
            "enable_log": staticmethod(lambda: None),
            "Collector": FakeCollector,
            "DaemonTypes": FakeDaemonTypes,
            "Schedd": FakeSchedd,
            "Submit": FakeSubmit,
        },
    )
    monkeypatch.setattr(condor_job_executor_module, "htcondor2", fake_htcondor)
    monkeypatch.setattr(
        condor_job_executor_module,
        "extractJDL",
        lambda raw_jdl: '[ Executable = "/bin/echo"; Arguments = "hello"; ]',
    )

    deps = make_dependencies()
    deps["job_db"].search.return_value = (1, [{"JobID": 42}])
    deps["job_db"].get_job_jdls.return_value = {42: "eJyFakeCompressedPayload"}

    result = await CondorJobExecutorTask(job_id=42).submit_to_condor(
        config=deps["config"],
        job_db=deps["job_db"],
    )

    assert result.cluster_id == 12345
    assert result.proc_id == 0
    assert result.schedd_name == condor_job_executor_module._settings.schedd_name


async def test_monitor_moves_received_jobs_and_schedules_executors(monkeypatch):
    scheduled = []

    async def fake_schedule(self, **kwargs):
        scheduled.append(self.job_id)
        return "task-id"

    monkeypatch.setattr(CondorJobExecutorTask, "schedule", fake_schedule)
    deps = make_dependencies()
    deps["job_db"].search.return_value = (2, [{"JobID": 1}, {"JobID": 2}])

    result = await CondorJobExecutorMonitorTask().execute(**deps)

    assert result == 2
    deps["job_db"].search.assert_awaited_once()
    (search_spec,) = deps["job_db"].search.await_args.args[1]
    assert search_spec["parameter"] == "Status"
    assert search_spec["value"] == JobStatus.RECEIVED
    deps["job_db"].set_job_attributes.assert_awaited_once()
    waiting_updates = deps["job_db"].set_job_attributes.await_args.args[0]
    assert waiting_updates[1]["Status"] == JobStatus.WAITING
    assert waiting_updates[2]["Status"] == JobStatus.WAITING
    deps["job_logging_db"].insert_records.assert_awaited_once()
    assert scheduled == [1, 2]


async def test_monitor_does_nothing_without_received_jobs(monkeypatch):
    schedule_executor = AsyncMock()
    monkeypatch.setattr(CondorJobExecutorTask, "schedule", schedule_executor)
    deps = make_dependencies()
    deps["job_db"].search.return_value = (0, [])

    result = await CondorJobExecutorMonitorTask().execute(**deps)

    assert result == 0
    deps["job_db"].set_job_attributes.assert_not_awaited()
    deps["job_logging_db"].insert_records.assert_not_awaited()
    schedule_executor.assert_not_awaited()
