"""Tests for the dummy job executor task."""

from __future__ import annotations

import logging

from diracx.tasks.jobs import DummyJobExecutorTask
from diracx.tasks.jobs.dummy_job_executor import (
    DummyJobExecutorTask as DummyJobExecutorTaskImplementation,
)


def test_dummy_job_executor_task_serializes_job_id():
    task = DummyJobExecutorTask(job_id=42)

    assert task.job_id == 42
    assert task.serialize() == (42,)


async def test_dummy_job_executor_task_execute(caplog):
    task = DummyJobExecutorTask(job_id=42)

    with caplog.at_level(logging.INFO, logger="diracx.tasks.jobs.dummy_job_executor"):
        result = await task.execute()

    assert result == 42
    assert caplog.record_tuples == [
        (
            "diracx.tasks.jobs.dummy_job_executor",
            logging.INFO,
            "I am executing 42",
        )
    ]


def test_dummy_job_executor_task_is_publicly_exported():
    assert DummyJobExecutorTask is DummyJobExecutorTaskImplementation
