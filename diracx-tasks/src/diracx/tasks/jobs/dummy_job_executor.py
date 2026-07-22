"""Tasks that simulate the execution of jobs, for use in demo deployments."""

from __future__ import annotations

import dataclasses
import logging
from datetime import UTC, datetime, timedelta

from pydantic import PositiveInt

from diracx.core.models import (
    JobStatus,
    JobStatusUpdate,
    ScalarSearchOperator,
    ScalarSearchSpec,
)
from diracx.core.settings import ServiceSettingsBase
from diracx.db.os import JobParametersDB
from diracx.db.sql import JobDB, JobLoggingDB, TaskQueueDB
from diracx.logic.jobs import set_job_statuses
from diracx.tasks.plumbing.base_task import BaseTask, PeriodicBaseTask
from diracx.tasks.plumbing.depends import Config
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import JOB
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import IntervalSeconds

logger = logging.getLogger(__name__)

MINOR_STATUS = "DummyExecutor"


class DummyJobExecutorSettings(ServiceSettingsBase):
    """Settings controlling automatic dummy job execution."""

    model_config = ServiceSettingsBase.model_config | {
        "env_prefix": "DIRACX_TASKS_DUMMY_JOB_EXECUTOR_",
        "use_attribute_docstrings": True,
    }

    enabled: bool = False
    """Whether the monitor is scheduled automatically."""

    interval_seconds: PositiveInt = 10
    """How often the enabled monitor searches for received jobs."""


_settings = DummyJobExecutorSettings()


@dataclasses.dataclass
class DummyJobExecutorMonitorTask(PeriodicBaseTask):
    """Periodically pick up newly received jobs and hand them to the dummy executor.

    When enabled, every job in ``Received`` state is moved to ``Waiting`` and a
    one-shot ``DummyJobExecutorTask`` is scheduled for it.
    """

    priority = Priority.BACKGROUND
    size = Size.SMALL
    _enabled = _settings.enabled
    default_schedule = IntervalSeconds(_settings.interval_seconds)

    async def execute(
        self,
        config: Config,
        job_db: JobDB,
        job_logging_db: JobLoggingDB,
        task_queue_db: TaskQueueDB,
        job_parameters_db: JobParametersDB,
    ) -> int:
        _, jobs = await job_db.search(
            ["JobID"],
            [
                ScalarSearchSpec(
                    parameter="Status",
                    operator=ScalarSearchOperator.EQUAL,
                    value=JobStatus.RECEIVED,
                )
            ],
            [],
        )
        if not jobs:
            return 0

        job_ids = [job["JobID"] for job in jobs]
        logger.info("Moving %d received job(s) to Waiting: %s", len(job_ids), job_ids)
        await set_job_statuses(
            {
                job_id: {
                    datetime.now(UTC): JobStatusUpdate(
                        Status=JobStatus.WAITING,
                        MinorStatus=MINOR_STATUS,
                    )
                }
                for job_id in job_ids
            },
            config=config,
            job_db=job_db,
            job_logging_db=job_logging_db,
            task_queue_db=task_queue_db,
            job_parameters_db=job_parameters_db,
        )

        for job_id in job_ids:
            await DummyJobExecutorTask(job_id=job_id).schedule()

        return len(job_ids)


@dataclasses.dataclass
class DummyJobExecutorTask(BaseTask):
    """Simulate the execution of a single job.

    The job is walked through the state machine's valid path
    ``Waiting -> Matched -> Running -> Done``: a direct jump to ``Done`` would be
    silently rejected. ``set_job_statuses`` applies the timestamped updates in
    chronological order, so a single call with increasing timestamps walks the
    whole chain.
    """

    priority = Priority.NORMAL
    size = Size.LARGE
    retry_policy = ExponentialBackoff(base_delay_seconds=10, max_retries=3)

    job_id: int

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(JOB, self.job_id)]

    async def execute(
        self,
        config: Config,
        job_db: JobDB,
        job_logging_db: JobLoggingDB,
        task_queue_db: TaskQueueDB,
        job_parameters_db: JobParametersDB,
    ) -> int:
        logger.info("Simulating execution of job %d", self.job_id)
        now = datetime.now(UTC)
        await set_job_statuses(
            {
                self.job_id: {
                    now: JobStatusUpdate(
                        Status=JobStatus.MATCHED,
                        MinorStatus=MINOR_STATUS,
                    ),
                    now + timedelta(seconds=1): JobStatusUpdate(
                        Status=JobStatus.RUNNING,
                        MinorStatus=MINOR_STATUS,
                    ),
                    now + timedelta(seconds=5): JobStatusUpdate(
                        Status=JobStatus.DONE,
                        MinorStatus=MINOR_STATUS,
                    ),
                }
            },
            config=config,
            job_db=job_db,
            job_logging_db=job_logging_db,
            task_queue_db=task_queue_db,
            job_parameters_db=job_parameters_db,
        )
        return self.job_id
