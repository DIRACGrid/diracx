"""Tests for the context added to the logs of the tasks."""

from __future__ import annotations

import io
import json
import logging
from unittest.mock import AsyncMock, patch

import pytest

from diracx.core import logs
from diracx.core.logs import configure_logging
from diracx.core.settings import LoggingSettings
from diracx.tasks.plumbing.broker.models import TaskMessage
from diracx.tasks.plumbing.worker.worker import Worker


@pytest.fixture
def json_logs(monkeypatch):
    root = logging.getLogger()
    saved = [
        (lg, lg.handlers[:], lg.level) for lg in (root, logging.getLogger("diracx"))
    ]
    monkeypatch.setattr(logs, "_handler", None)
    handler = configure_logging(LoggingSettings(format="json"))
    # Write to our own buffer: the stream bound at setup is not the one capsys reads
    stream = io.StringIO()
    handler.setStream(stream)
    yield stream
    for lg, handlers, level in saved:
        lg.handlers = handlers
        lg.setLevel(level)


async def test_worker_logs_carry_the_task(
    json_logs, broker, task_class_registry, wrapped_registry
):
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )
    message = TaskMessage(
        task_id="t-logs",
        task_name="test:SuccessTask",
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
    )
    redis = AsyncMock()
    redis.__aenter__ = AsyncMock(return_value=redis)
    redis.__aexit__ = AsyncMock(return_value=False)
    redis.set = AsyncMock(return_value=True)

    with patch.object(worker, "_get_redis", return_value=redis):
        await worker.process_message(message.dumpb())
    logging.getLogger("diracx.test_logs").info("after the task")

    entries = [
        json.loads(line)
        for line in json_logs.getvalue().splitlines()
        if line.startswith("{")
    ]
    [executing] = [e for e in entries if e["body"].startswith("Executing task")]
    assert executing["task.name"] == "test:SuccessTask"
    assert executing["task.id"] == "t-logs"
    [after] = [e for e in entries if e["body"] == "after the task"]
    assert "task.name" not in after
