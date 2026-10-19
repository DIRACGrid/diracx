"""Tests for the task factory (wrapping, registry)."""

from __future__ import annotations

import dataclasses
import os
from typing import Any

import pytest

from diracx.db.exceptions import MissingDBURLError
from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.factory import wrap_task
from diracx.tasks.plumbing.lock_registry import TASK
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.task_run import start_worker


@dataclasses.dataclass
class SampleTask(BaseTask):
    value: int
    priority = Priority.NORMAL
    size = Size.SMALL

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(TASK, "SampleTask")]

    async def execute(self, **kwargs: Any) -> int:
        return self.value * 2


def test_wrap_task_creates_callable():
    wrapped = wrap_task(SampleTask)
    assert callable(wrapped)
    assert hasattr(wrapped, "_dependant")
    assert wrapped.__name__ == "SampleTask"


def test_wrap_task_signature():
    wrapped = wrap_task(SampleTask)
    import inspect

    sig = inspect.signature(wrapped)
    param_names = list(sig.parameters.keys())
    # Should have positional args and kwargs, but no 'self'
    assert "self" not in param_names


async def test_worker_refuses_to_start_without_db_urls(monkeypatch):
    """The worker names each installed task that uses a DB without a URL."""
    for name in list(os.environ):
        if name.startswith(("DIRACX_DB_URL_", "DIRACX_OS_DB_", "DIRACX_SERVICE_")):
            monkeypatch.delenv(name)

    # Raised before connecting to Redis
    with pytest.raises(MissingDBURLError) as exc_info:
        await start_worker("redis://127.0.0.1:1", 1, "small")
    assert "DIRACX_DB_URL_SANDBOXMETADATADB" in exc_info.value.env_vars
    # The dead letter queue is optional
    assert "DIRACX_DB_URL_TASKDB" not in exc_info.value.env_vars
