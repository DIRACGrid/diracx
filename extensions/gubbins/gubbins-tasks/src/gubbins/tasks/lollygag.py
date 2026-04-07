"""Example task implementations using the Gubbins LollygagDB.

Demonstrates:
- One-shot tasks with custom locks, retry policy, and dead letter queue
- Periodic tasks with IntervalSeconds schedule
- VO-aware periodic tasks with CronSchedule
- Custom ``LockedObjectType`` (``LOLLYGAG``) for domain-specific locking
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

from diracx.tasks.plumbing.base_task import (
    BaseTask,
    PeriodicBaseTask,
    PeriodicVoAwareBaseTask,
)
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import LockedObjectType
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import CronSchedule, IntervalSeconds

from gubbins.db.sql import LollygagDB
from gubbins.logic.lollygag.lollygag import get_owner_object, insert_owner_object

from .lock_types import LOLLYGAG

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SyncOwnersTask(BaseTask):
    """Sync an owner entry into the Lollygag database.

    Uses a per-owner mutex lock so two syncs of the same owner are
    mutually exclusive, but different owners can sync in parallel.
    Retries up to 3 times with exponential backoff on failure.
    """

    owner_name: str

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = ExponentialBackoff(base_delay_seconds=5, max_retries=3)
    dlq_eligible = True

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(LockedObjectType(LOLLYGAG), self.owner_name)]

    async def execute(  # type: ignore[override]
        self, lollygag_db: LollygagDB, **kwargs: Any
    ) -> int:
        owner_id = await insert_owner_object(lollygag_db, self.owner_name)
        logger.info("Synced owner %s (id=%d)", self.owner_name, owner_id)
        return owner_id


class OwnerCleanupTask(PeriodicBaseTask):
    """Periodically log the current set of owners.

    Runs every hour. Uses the default MutexLock from PeriodicBaseTask.
    """

    default_schedule = IntervalSeconds(3600)

    async def execute(  # type: ignore[override]
        self, lollygag_db: LollygagDB, **kwargs: Any
    ) -> list[str]:
        owners = await get_owner_object(lollygag_db)
        logger.info("Current owners: %s", owners)
        return owners


@dataclasses.dataclass
class OwnerReportTask(PeriodicVoAwareBaseTask):
    """Per-VO periodic task that reports owners.

    Demonstrates VO-aware scheduling: the scheduler creates one instance
    per VO, each with its own cron schedule and VO-scoped mutex lock.
    """

    vo: str

    default_schedule = CronSchedule("0 6 * * *")  # daily at 06:00

    async def execute(  # type: ignore[override]
        self, lollygag_db: LollygagDB, **kwargs: Any
    ) -> list[str]:
        owners = await get_owner_object(lollygag_db)
        logger.info("VO %s owner report: %s", self.vo, owners)
        return owners
