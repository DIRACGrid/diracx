"""Tasks that hand submitted jobs to an HTCondor scheduler."""

from __future__ import annotations

import dataclasses
import logging
from datetime import UTC, datetime

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

MINOR_STATUS = "CondorExecutor"


class CondorJobExecutorSettings(ServiceSettingsBase):
    """Settings controlling automatic Condor job submission."""

    model_config = ServiceSettingsBase.model_config | {
        "env_prefix": "DIRACX_TASKS_CONDOR_JOB_EXECUTOR_",
        "use_attribute_docstrings": True,
    }

    enabled: bool = False
    """Whether the monitor is scheduled automatically."""

    interval_seconds: PositiveInt = 10
    """How often the enabled monitor searches for received jobs."""

    schedd_name: str = "default"
    """Logical name of the target HTCondor schedd."""

    collector_host: str | None = None
    """Optional collector host used to resolve the target schedd."""


_settings = CondorJobExecutorSettings()


@dataclasses.dataclass(frozen=True)
class CondorSubmitResult:
    cluster_id: int
    proc_id: int = 0
    schedd_name: str | None = None


@dataclasses.dataclass
class CondorJobExecutorMonitorTask(PeriodicBaseTask):
    """Move newly received jobs to Waiting and queue Condor submission tasks."""

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
                        Source=MINOR_STATUS,
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
            await CondorJobExecutorTask(job_id=job_id).schedule()

        return len(job_ids)


@dataclasses.dataclass
class CondorJobExecutorTask(BaseTask):
    """Submit a single job to the configured HTCondor schedd."""

    priority = Priority.NORMAL
    size = Size.LARGE
    retry_policy = ExponentialBackoff(base_delay_seconds=10, max_retries=3)

    job_id: int

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(JOB, self.job_id)]

    async def submit_to_condor(
        self,
        *,
        config: Config,
        job_db: JobDB,
    ) -> CondorSubmitResult:
        target = _settings.schedd_name
        if _settings.collector_host:
            target = f"{target}@{_settings.collector_host}"

        raise NotImplementedError(
            f"Implement HTCondor submission for schedd '{target}' in "
            "CondorJobExecutorTask.submit_to_condor"
        )

    async def execute(
        self,
        config: Config,
        job_db: JobDB,
        job_logging_db: JobLoggingDB,
        task_queue_db: TaskQueueDB,
        job_parameters_db: JobParametersDB,
    ) -> int:
        logger.info("Submitting job %d to HTCondor", self.job_id)
        submission = await self.submit_to_condor(config=config, job_db=job_db)
        target = submission.schedd_name or _settings.schedd_name
        application_status = (
            f"Submitted to HTCondor schedd {target} as "
            f"{submission.cluster_id}.{submission.proc_id}"
        )
        now = datetime.now(UTC)
        await set_job_statuses(
            {
                self.job_id: {
                    now: JobStatusUpdate(
                        Status=JobStatus.MATCHED,
                        MinorStatus=MINOR_STATUS,
                        ApplicationStatus=application_status,
                        Source=MINOR_STATUS,
                    )
                }
            },
            config=config,
            job_db=job_db,
            job_logging_db=job_logging_db,
            task_queue_db=task_queue_db,
            job_parameters_db=job_parameters_db,
        )
        return self.job_id
