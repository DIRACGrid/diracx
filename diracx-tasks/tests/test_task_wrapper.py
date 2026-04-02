"""Tests for task_wrapper with Redis injection and interactive mode."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.exceptions import UnableToAcquireLockError
from diracx.tasks.plumbing.factory import task_wrapper
from diracx.tasks.plumbing.lock_registry import TASK
from diracx.tasks.plumbing.locks import BaseLock, MutexLock, RateLimiter

from .conftest import LockedTask


async def test_task_wrapper_no_redis_skips_locks():
    """When _redis is None, locks should be skipped with a warning."""
    result = await task_wrapper(LockedTask, _redis=None)
    assert result == "locked_ok"


async def test_task_wrapper_with_redis_acquires_locks():
    """When _redis is provided, locks should be acquired."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)  # MutexLock.acquire
    mock_redis.eval = AsyncMock(return_value=1)  # MutexLock.release

    result = await task_wrapper(LockedTask, _redis=mock_redis)
    assert result == "locked_ok"

    # Should have called set (acquire) and eval (release)
    mock_redis.set.assert_called_once()
    mock_redis.eval.assert_called_once()


async def test_task_wrapper_lock_failure_raises():
    """When a lock can't be acquired, UnableToAcquireLockError should be raised."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=None)  # Lock not acquired

    with pytest.raises(UnableToAcquireLockError):
        await task_wrapper(LockedTask, _redis=mock_redis)


async def test_task_wrapper_interactive_skips_limiters():
    """In interactive mode, BaseLimiter subclasses should be skipped."""

    class TaskWithLimiter(BaseTask):
        priority = Priority.NORMAL
        size = Size.SMALL

        @property
        def execution_locks(self) -> list[BaseLock]:
            limiter = RateLimiter(TASK, "TaskWithLimiter")
            limiter.limit = 1
            limiter.window_seconds = 60
            return [
                MutexLock(TASK, "TaskWithLimiter"),
                limiter,
            ]

        async def execute(self, **kwargs: Any) -> str:
            return "with_limiter"

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)  # Mutex succeeds
    mock_redis.eval = AsyncMock(return_value=1)  # Release

    # In interactive mode, the RateLimiter should be skipped
    result = await task_wrapper(TaskWithLimiter, _redis=mock_redis, _interactive=True)
    assert result == "with_limiter"

    # Only mutex should have been acquired (set call), not the rate limiter
    assert mock_redis.set.call_count == 1


async def test_task_wrapper_non_interactive_acquires_limiters():
    """In non-interactive mode, limiters should be acquired normally."""

    class TaskWithLimiter(BaseTask):
        priority = Priority.NORMAL
        size = Size.SMALL

        @property
        def execution_locks(self) -> list[BaseLock]:
            limiter = RateLimiter(TASK, "TaskWithLimiter2")
            limiter.limit = 10
            limiter.window_seconds = 60
            return [limiter]

        async def execute(self, **kwargs: Any) -> str:
            return "with_limiter"

    mock_redis = AsyncMock()
    # RateLimiter uses eval for its Lua script
    mock_redis.eval = AsyncMock(return_value=1)

    result = await task_wrapper(TaskWithLimiter, _redis=mock_redis, _interactive=False)
    assert result == "with_limiter"

    # Rate limiter should have been acquired via eval
    mock_redis.eval.assert_called()


async def test_task_wrapper_releases_on_exception():
    """Locks should be released even when the task raises."""

    class FailingLockedTask(BaseTask):
        priority = Priority.NORMAL
        size = Size.SMALL

        @property
        def execution_locks(self) -> list[BaseLock]:
            return [MutexLock(TASK, "FailingLocked")]

        async def execute(self, **kwargs: Any) -> str:
            raise ValueError("task failed")

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.eval = AsyncMock(return_value=1)

    with pytest.raises(ValueError, match="task failed"):
        await task_wrapper(FailingLockedTask, _redis=mock_redis)

    # Release should still have been called
    mock_redis.eval.assert_called_once()
