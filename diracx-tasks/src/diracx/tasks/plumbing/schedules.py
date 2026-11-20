"""Schedules computing the next run time of periodic tasks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta

from croniter import croniter
from dateutil.rrule import rrulestr


class TaskScheduleBase(ABC):
    """Abstract base class for task scheduling."""

    @abstractmethod
    def next_occurrence(self) -> datetime:
        """Return the datetime for the next scheduled occurrence.

        Returns:
            The timezone-aware time of the next occurrence.
        """
        ...


class IntervalSeconds(TaskScheduleBase):
    """Schedule that repeats at a fixed interval."""

    def __init__(self, seconds: int):
        self.seconds = seconds

    def next_occurrence(self) -> datetime:
        """Return the time one interval from now.

        Returns:
            The timezone-aware time of the next occurrence.
        """
        return datetime.now(tz=UTC) + timedelta(seconds=self.seconds)


class CronSchedule(TaskScheduleBase):
    """Schedule based on a cron expression."""

    def __init__(self, expression: str):
        self.expression = expression

    def next_occurrence(self) -> datetime:
        """Return the next time matching the cron expression.

        Returns:
            The timezone-aware time of the next occurrence.
        """
        cron = croniter(self.expression, datetime.now(tz=UTC))
        return cron.get_next(datetime)


class RRuleSchedule(TaskScheduleBase):
    """Schedule based on an RFC 2445 recurrence rule."""

    def __init__(self, rule: str):
        self.rule = rule

    def next_occurrence(self) -> datetime:
        """Return the next time matching the recurrence rule.

        Returns:
            The timezone-aware time of the next occurrence.

        Raises:
            ValueError: If the rule has no future occurrences.
        """
        now = datetime.now(tz=UTC)
        rrule = rrulestr(self.rule, dtstart=now)
        result = rrule.after(now)
        if result is None:
            raise ValueError(f"RRule {self.rule!r} has no future occurrences")
        return result
