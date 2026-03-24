"""Task definition to clean Sandbox Store."""

from __future__ import annotations

import dataclasses
from typing import Annotated

from diracx.core.settings import SandboxStoreSettings
from diracx.db.sql import SandboxMetadataDB
from diracx.logic.jobs.sandboxes import clean_sandboxes
from diracx.tasks.plumbing.base_task import PeriodicBaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import CronSchedule

from .depends import NoTransaction


@dataclasses.dataclass
class CleanSandboxStoreTask(PeriodicBaseTask):
    priority = Priority.BACKGROUND
    size = Size.MEDIUM
    retry_policy = ExponentialBackoff(base_delay_seconds=300, max_retries=3)
    default_schedule = CronSchedule("0 6 * * *")

    async def execute(
        self,
        sandbox_metadata_db: Annotated[SandboxMetadataDB, NoTransaction()],
        settings: SandboxStoreSettings,
    ) -> int:
        return await clean_sandboxes(sandbox_metadata_db, settings)
