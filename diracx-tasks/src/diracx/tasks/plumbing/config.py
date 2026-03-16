from __future__ import annotations

__all__ = ["TasksConfig", "TaskOverride", "PeriodicTaskConfig"]


from pydantic import BaseModel


class PeriodicTaskConfig(BaseModel):
    """Configuration for a periodic task from the DiracX CS."""

    enabled: bool = True
    schedule: str | None = None  # Cron expression override
    interval_seconds: int | None = None  # Interval override


class TaskOverride(BaseModel):
    """Per-task configuration overrides."""

    enabled: bool = True
    rate_limit: int | None = None
    rate_window_seconds: int | None = None
    concurrency_limit: int | None = None
    periodic: PeriodicTaskConfig | None = None


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
    vo_overrides: dict[str, dict[str, TaskOverride]] = {}

    def get_override(self, task_name: str, vo: str | None = None) -> TaskOverride:
        """Get effective config for a task, with VO overrides merged."""
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
