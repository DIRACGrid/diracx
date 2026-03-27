"""Integration tests for the TaskScheduler."""

from __future__ import annotations

import dataclasses
from typing import Any
from unittest.mock import MagicMock

from diracx.tasks.plumbing.base_task import (
    PeriodicBaseTask,
    PeriodicVoAwareBaseTask,
)
from diracx.tasks.plumbing.scheduler.scheduler import TaskScheduler
from diracx.tasks.plumbing.schedules import IntervalSeconds

from .conftest import get_enqueued_messages


class MyPeriodicTask(PeriodicBaseTask):
    default_schedule = IntervalSeconds(60)

    async def execute(self, **kwargs: Any) -> str:
        return "periodic_result"


@dataclasses.dataclass
class MyVoAwareTask(PeriodicVoAwareBaseTask):
    vo: str
    default_schedule = IntervalSeconds(120)

    async def execute(self, **kwargs: Any) -> str:
        return f"vo_result_{self.vo}"


def test_compute_initial_schedules_periodic(broker):
    """Periodic tasks should get an initial schedule entry."""
    scheduler = TaskScheduler(
        broker=broker,
        redis_url="redis://unused",
        task_registry={"jobs:MyPeriodicTask": MyPeriodicTask},
    )
    scheduler._compute_initial_schedules()
    assert ("jobs:MyPeriodicTask", "") in scheduler._next_runs


def test_compute_initial_schedules_skips_disabled(broker):
    """Disabled tasks should not get schedule entries."""

    class DisabledTask(PeriodicBaseTask):
        default_schedule = IntervalSeconds(60)
        _enabled = False

        async def execute(self, **kwargs: Any) -> str:
            return "never"

    scheduler = TaskScheduler(
        broker=broker,
        redis_url="redis://unused",
        task_registry={"jobs:DisabledTask": DisabledTask},
    )
    scheduler._compute_initial_schedules()
    assert len(scheduler._next_runs) == 0


def test_compute_initial_schedules_vo_aware_no_config(broker):
    """VO-aware tasks with no config should be skipped with a warning."""
    scheduler = TaskScheduler(
        broker=broker,
        redis_url="redis://unused",
        task_registry={"jobs:MyVoAwareTask": MyVoAwareTask},
        config=None,
    )
    scheduler._compute_initial_schedules()
    # No VOs available, so no schedules
    assert len(scheduler._next_runs) == 0


def test_compute_initial_schedules_vo_aware_with_config(broker):
    """VO-aware tasks with config should create one entry per VO."""
    # Mock a Config object with Registry containing VOs
    mock_config = MagicMock()
    mock_config.Registry = {"lhcb": MagicMock(), "atlas": MagicMock()}

    scheduler = TaskScheduler(
        broker=broker,
        redis_url="redis://unused",
        task_registry={"jobs:MyVoAwareTask": MyVoAwareTask},
        config=mock_config,
    )
    scheduler._compute_initial_schedules()

    # Should have entries for both VOs
    assert ("jobs:MyVoAwareTask", "lhcb") in scheduler._next_runs
    assert ("jobs:MyVoAwareTask", "atlas") in scheduler._next_runs
    assert len(scheduler._next_runs) == 2


def test_load_vos_without_config(broker):
    """load_vos should return empty list when no config."""
    scheduler = TaskScheduler(broker=broker, redis_url="redis://unused", config=None)
    assert scheduler.load_vos() == []


def test_load_vos_with_config(broker):
    """load_vos should return VO names from config Registry."""
    mock_config = MagicMock()
    mock_config.Registry = {"vo1": MagicMock(), "vo2": MagicMock(), "vo3": MagicMock()}

    scheduler = TaskScheduler(
        broker=broker, redis_url="redis://unused", config=mock_config
    )
    vos = scheduler.load_vos()
    assert sorted(vos) == ["vo1", "vo2", "vo3"]


def test_add_vo_schedule(broker):
    """add_vo_schedule should register a schedule entry."""
    from datetime import UTC, datetime

    scheduler = TaskScheduler(broker=broker, redis_url="redis://unused")

    now = datetime.now(tz=UTC)
    scheduler.add_vo_schedule("test:Task", "lhcb", now)

    assert ("test:Task", "lhcb") in scheduler._next_runs
    assert scheduler._next_runs[("test:Task", "lhcb")] == now


async def test_submit_periodic_task(broker):
    """_submit_periodic_task should enqueue a message to the broker."""
    scheduler = TaskScheduler(
        broker=broker,
        redis_url="redis://unused",
        task_registry={"jobs:MyPeriodicTask": MyPeriodicTask},
    )

    await scheduler._submit_periodic_task("jobs:MyPeriodicTask", "")

    messages = await get_enqueued_messages(broker)
    assert len(messages) == 1
    assert messages[0].task_name == "jobs:MyPeriodicTask"
    assert messages[0].labels["periodic"] is True


async def test_submit_vo_aware_periodic_task(broker):
    """_submit_periodic_task with a VO should include vo in labels and args."""
    scheduler = TaskScheduler(
        broker=broker,
        redis_url="redis://unused",
        task_registry={"jobs:MyVoAwareTask": MyVoAwareTask},
    )

    await scheduler._submit_periodic_task("jobs:MyVoAwareTask", "lhcb")

    messages = await get_enqueued_messages(broker)
    assert len(messages) == 1
    assert messages[0].labels["vo"] == "lhcb"
    # The VO should be in the task message args
    assert messages[0].task_args == ["lhcb"]
