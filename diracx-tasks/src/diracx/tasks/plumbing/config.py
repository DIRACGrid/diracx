"""Pydantic models for the task configuration stored in the DiracX CS."""

from __future__ import annotations

from pydantic import BaseModel


class PeriodicTaskConfig(BaseModel):
    """Configuration for a periodic task from the DiracX CS."""

    enabled: bool = True
    """Whether the periodic task is scheduled."""

    schedule: str | None = None
    """Cron expression overriding the default schedule."""

    interval_seconds: int | None = None
    """Interval overriding the default schedule."""


class TaskOverride(BaseModel):
    """Per-task configuration overrides."""

    enabled: bool = True
    """Whether the task is enabled."""

    rate_limit: int | None = None
    """Maximum number of executions per rate window."""

    rate_window_seconds: int | None = None
    """Length of the rate window, in seconds."""

    concurrency_limit: int | None = None
    """Maximum number of concurrent executions."""

    periodic: PeriodicTaskConfig | None = None
    """Overrides specific to periodic tasks."""


class TasksConfig(BaseModel):
    """Top-level task configuration from DiracX CS.

    Structure in YAML:
    ```yaml
    tasks:
      common:
        TaskClassName:
          enabled: true
          rate_limit: 100
          rate_window_seconds: 60
      vo-overrides:
        lhcb:
          TaskClassName:
            rate_limit: 200
    ```
    """

    common: dict[str, TaskOverride] = {}
    """Overrides applying to all VOs, keyed by task name."""

    vo_overrides: dict[str, dict[str, TaskOverride]] = {}
    """Overrides per VO, keyed by VO then task name."""

    def get_override(self, task_name: str, vo: str | None = None) -> TaskOverride:
        """Get effective config for a task, with VO overrides merged.

        Args:
            task_name: Name of the task class.
            vo: Name of the VO whose overrides take precedence over the
                common ones; only common overrides apply if omitted.

        Returns:
            The merged overrides, or defaults if none are configured.
        """
        base = self.common.get(task_name, TaskOverride())
        if vo and vo in self.vo_overrides:
            vo_override = self.vo_overrides[vo].get(task_name)
            if vo_override:
                # Merge: VO values take precedence over common
                merged = base.model_dump()
                for field, value in vo_override.model_dump(exclude_unset=True).items():
                    if value is not None:
                        merged[field] = value
                return TaskOverride.model_validate(merged)
        return base
