"""Task that simulates executing a job."""

from __future__ import annotations

import dataclasses
import logging

from diracx.tasks.plumbing.base_task import BaseTask
from diracx.tasks.plumbing.enums import Size

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DummyJobExecutorTask(BaseTask):
    """Log execution of a single job."""

    job_id: int

    size = Size.SMALL

    async def execute(self) -> int:
        logger.info("I am executing %d", self.job_id)
        return self.job_id
