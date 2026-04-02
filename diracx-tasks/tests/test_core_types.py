"""Tests for core types: enums, locks, retry policies, schedules, config."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import (
    JOB,
    TASK,
    TRANSFORMATION,
    LockedObjectType,
    register_locked_object_type,
)
from diracx.tasks.plumbing.locks import (
    ConcurrencyLimiter,
    ExclusiveRWLock,
    MutexLock,
    RateLimiter,
    SharedRWLock,
)
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff, NoRetry
from diracx.tasks.plumbing.schedules import CronSchedule, IntervalSeconds


def test_priority_enum():
    assert Priority.REALTIME == "realtime"
    assert Priority.NORMAL == "normal"
    assert Priority.BACKGROUND == "background"


def test_size_enum():
    assert Size.SMALL == "small"
    assert Size.MEDIUM == "medium"
    assert Size.LARGE == "large"


def test_locked_object_type_builtin():
    assert TASK == "task"
    assert JOB == "job"
    assert TRANSFORMATION == "transformation"
    assert isinstance(TASK, LockedObjectType)
    assert isinstance(JOB, LockedObjectType)
    assert isinstance(TRANSFORMATION, LockedObjectType)


def test_locked_object_type_unknown():
    with pytest.raises(ValueError, match="Unknown LockedObjectType"):
        LockedObjectType("nonexistent")


def test_locked_object_type_custom():
    register_locked_object_type("custom_test_type")
    assert LockedObjectType("custom_test_type") == "custom_test_type"


def test_mutex_lock_redis_key():
    lock = MutexLock(TASK, "MyTask")
    assert lock.redis_key == "lock:mutex:task:MyTask"


def test_mutex_lock_redis_key_extra():
    lock = MutexLock(TRANSFORMATION, 123, "extra")
    assert lock.redis_key == "lock:mutex:transformation:123:extra"


def test_shared_rw_lock_key():
    lock = SharedRWLock(TRANSFORMATION, 42)
    assert lock.redis_key == "lock:rw:transformation:42"


def test_exclusive_rw_lock_key():
    lock = ExclusiveRWLock(TRANSFORMATION, 42)
    assert lock.redis_key == "lock:rw:transformation:42"


def test_rate_limiter_key():
    limiter = RateLimiter(TASK, "MyTask")
    assert limiter.redis_key == "limiter:rate:task:MyTask"


def test_concurrency_limiter_key():
    limiter = ConcurrencyLimiter(TASK, "MyTask")
    assert limiter.redis_key == "limiter:conc:task:MyTask"


def test_no_retry_policy():
    policy = NoRetry()
    assert policy.schedule_retry(1, Exception("test")) is None


def test_exponential_backoff():
    policy = ExponentialBackoff(base_delay_seconds=10, max_retries=3)

    # Should retry for attempt 0, 1, 2
    result = policy.schedule_retry(0, Exception("test"))
    assert result is not None
    assert result > datetime.now(tz=UTC)

    result = policy.schedule_retry(2, Exception("test"))
    assert result is not None

    # Should stop at max_retries
    result = policy.schedule_retry(3, Exception("test"))
    assert result is None


def test_exponential_backoff_delay_increases():
    policy = ExponentialBackoff(base_delay_seconds=10, max_retries=5)

    result1 = policy.schedule_retry(0, Exception())
    result2 = policy.schedule_retry(1, Exception())

    # Second retry should be later than first
    assert result1 is not None
    assert result2 is not None
    assert result2 > result1


def test_interval_seconds_schedule():
    schedule = IntervalSeconds(seconds=300)
    now = datetime.now(tz=UTC)
    next_run = schedule.next_occurrence()
    assert next_run > now
    assert next_run <= now + timedelta(seconds=301)


def test_cron_schedule():
    schedule = CronSchedule("*/5 * * * *")
    now = datetime.now(tz=UTC)
    next_run = schedule.next_occurrence()
    assert next_run > now
    # Should be within 5 minutes
    assert next_run <= now + timedelta(minutes=5, seconds=1)
