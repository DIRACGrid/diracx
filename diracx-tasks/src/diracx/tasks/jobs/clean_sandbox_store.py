"""Task definition to clean Sandbox Store."""

from __future__ import annotations

import dataclasses
from typing import Annotated

from diracx.core.settings import SandboxStoreSettings
from diracx.db.sql import SandboxMetadataDB
from diracx.logic.jobs.sandboxes import clean_sandboxes
from diracx.tasks.depends import NoTransaction
from diracx.tasks.plumbing.base_task import PeriodicBaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import CronSchedule


@dataclasses.dataclass
class CleanSandboxStoreTask(PeriodicBaseTask):
    """Periodic task that removes unused sandboxes from the Sandbox Store.

    Runs daily at 06:00 (cron schedule) with background priority and retries
    with exponential backoff on failure.
    """

    priority = Priority.BACKGROUND
    size = Size.MEDIUM
    retry_policy = ExponentialBackoff(base_delay_seconds=300, max_retries=3)
    default_schedule = CronSchedule("0 6 * * *")

    async def execute(
        self,
        sandbox_metadata_db: Annotated[SandboxMetadataDB, NoTransaction()],
        settings: SandboxStoreSettings,
    ) -> int:
        """Delete sandboxes that are not assigned to any job.

        Args:
            sandbox_metadata_db: Sandbox metadata database, accessed without
                an implicit transaction.
            settings: Sandbox store settings.

        Returns:
            The total number of sandboxes deleted.
        """
        return await clean_sandboxes(sandbox_metadata_db, settings)
