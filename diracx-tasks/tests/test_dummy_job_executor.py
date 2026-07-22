"""Tests for the dummy job executor tasks."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from diracx.core.models import JobStatus
from diracx.tasks.jobs import DummyJobExecutorMonitorTask, DummyJobExecutorTask
from diracx.tasks.jobs import dummy_job_executor as dummy_job_executor_module
from diracx.tasks.plumbing.locks import MutexLock

FEATURE_ENABLED_ENV = "DIRACX_TASKS_DUMMY_JOB_EXECUTOR_ENABLED"
FEATURE_INTERVAL_ENV = "DIRACX_TASKS_DUMMY_JOB_EXECUTOR_INTERVAL_SECONDS"
SCHEDULER_STATE_SCRIPT = """
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

from diracx.tasks.plumbing.factory import load_task_registry
from diracx.tasks.plumbing.scheduler import TaskScheduler

task_name = "jobs:DummyJobExecutorMonitorTask"
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
        "job_logging_db": MagicMock(name="job_logging_db"),
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
        dummy_job_executor_module._DummyJobExecutorSettings(interval_seconds=0)


def test_executor_takes_a_per_job_mutex():
    locks = DummyJobExecutorTask(job_id=42).execution_locks

    assert len(locks) == 1
    assert isinstance(locks[0], MutexLock)
    assert locks[0].redis_key == "lock:mutex:job:42"


async def test_executor_walks_job_through_the_state_machine(monkeypatch):
    set_job_statuses = AsyncMock()
    monkeypatch.setattr(dummy_job_executor_module, "set_job_statuses", set_job_statuses)
    deps = make_dependencies()

    result = await DummyJobExecutorTask(job_id=42).execute(**deps)

    assert result == 42
    set_job_statuses.assert_awaited_once()
    status_changes = set_job_statuses.await_args.args[0]
    assert set(status_changes) == {42}
    updates = status_changes[42]
    assert list(updates) == sorted(updates), "timestamps must be increasing"
    assert [update.status for update in updates.values()] == [
        JobStatus.MATCHED,
        JobStatus.RUNNING,
        JobStatus.DONE,
    ]


async def test_monitor_moves_received_jobs_and_schedules_executors(monkeypatch):
    set_job_statuses = AsyncMock()
    monkeypatch.setattr(dummy_job_executor_module, "set_job_statuses", set_job_statuses)
    scheduled = []

    async def fake_schedule(self, **kwargs):
        scheduled.append(self.job_id)
        return "task-id"

    monkeypatch.setattr(DummyJobExecutorTask, "schedule", fake_schedule)
    deps = make_dependencies()
    deps["job_db"].search.return_value = (2, [{"JobID": 1}, {"JobID": 2}])

    result = await DummyJobExecutorMonitorTask().execute(**deps)

    assert result == 2
    deps["job_db"].search.assert_awaited_once()
    (search_spec,) = deps["job_db"].search.await_args.args[1]
    assert search_spec["parameter"] == "Status"
    assert search_spec["value"] == JobStatus.RECEIVED
    set_job_statuses.assert_awaited_once()
    status_changes = set_job_statuses.await_args.args[0]
    assert set(status_changes) == {1, 2}
    for updates in status_changes.values():
        assert [update.status for update in updates.values()] == [JobStatus.WAITING]
    assert scheduled == [1, 2]


async def test_monitor_does_nothing_without_received_jobs(monkeypatch):
    set_job_statuses = AsyncMock()
    monkeypatch.setattr(dummy_job_executor_module, "set_job_statuses", set_job_statuses)
    schedule_executor = AsyncMock()
    monkeypatch.setattr(DummyJobExecutorTask, "schedule", schedule_executor)
    deps = make_dependencies()
    deps["job_db"].search.return_value = (0, [])

    result = await DummyJobExecutorMonitorTask().execute(**deps)

    assert result == 0
    set_job_statuses.assert_not_awaited()
    schedule_executor.assert_not_awaited()
