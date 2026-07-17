"""Tests for the dummy job executor tasks."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from diracx.core.models import JobStatus
from diracx.tasks.jobs import DummyJobExecutorMonitorTask, DummyJobExecutorTask
from diracx.tasks.jobs import dummy_job_executor as dummy_job_executor_module
from diracx.tasks.plumbing.locks import MutexLock
from diracx.tasks.plumbing.schedules import IntervalSeconds


def make_dependencies():
    return {
        "config": MagicMock(name="config"),
        "job_db": AsyncMock(name="job_db"),
        "job_logging_db": MagicMock(name="job_logging_db"),
        "task_queue_db": MagicMock(name="task_queue_db"),
        "job_parameters_db": MagicMock(name="job_parameters_db"),
    }


def test_executor_serializes_job_id():
    task = DummyJobExecutorTask(job_id=42)

    assert task.job_id == 42
    assert task.serialize() == (42,)


def test_monitor_serializes_to_empty_tuple():
    assert DummyJobExecutorMonitorTask().serialize() == ()


def test_monitor_runs_periodically():
    schedule = DummyJobExecutorMonitorTask.default_schedule

    assert isinstance(schedule, IntervalSeconds)
    assert schedule.seconds == 10


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
    assert set_job_statuses.await_args.kwargs == {
        "config": deps["config"],
        "job_db": deps["job_db"],
        "job_logging_db": deps["job_logging_db"],
        "task_queue_db": deps["task_queue_db"],
        "job_parameters_db": deps["job_parameters_db"],
    }


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
    deps = make_dependencies()
    deps["job_db"].search.return_value = (0, [])

    result = await DummyJobExecutorMonitorTask().execute(**deps)

    assert result == 0
    set_job_statuses.assert_not_awaited()


def test_tasks_are_publicly_exported():
    assert DummyJobExecutorTask is dummy_job_executor_module.DummyJobExecutorTask
    assert (
        DummyJobExecutorMonitorTask
        is dummy_job_executor_module.DummyJobExecutorMonitorTask
    )
