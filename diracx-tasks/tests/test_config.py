"""Tests for task configuration parsing."""

from __future__ import annotations

from diracx.tasks.plumbing.config import TaskOverride, TasksConfig


def test_tasks_config_empty():
    config = TasksConfig()
    override = config.get_override("SomeTask")
    assert override.enabled is True
    assert override.rate_limit is None


def test_tasks_config_common():
    config = TasksConfig(
        common={
            "MyTask": TaskOverride(rate_limit=100, rate_window_seconds=60),
        }
    )
    override = config.get_override("MyTask")
    assert override.rate_limit == 100
    assert override.rate_window_seconds == 60


def test_tasks_config_vo_override():
    config = TasksConfig(
        common={
            "MyTask": TaskOverride(rate_limit=100, concurrency_limit=5),
        },
        vo_overrides={
            "lhcb": {
                "MyTask": TaskOverride(rate_limit=200),
            },
        },
    )
    # Without VO
    override = config.get_override("MyTask")
    assert override.rate_limit == 100
    assert override.concurrency_limit == 5

    # With VO override
    override = config.get_override("MyTask", vo="lhcb")
    assert override.rate_limit == 200
    assert override.concurrency_limit == 5  # Inherited from common
