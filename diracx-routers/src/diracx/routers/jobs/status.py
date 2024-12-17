from __future__ import annotations

import logging
from datetime import datetime
from http import HTTPStatus
from typing import Annotated

from fastapi import BackgroundTasks, HTTPException, Query

from diracx.core.models import (
    JobStatusUpdate,
    SetJobStatusReturn,
)
from diracx.db.sql.utils.job import (
    remove_jobs,
    reschedule_jobs_bulk,
    set_job_status_bulk,
)

from ..dependencies import (
    Config,
    JobDB,
    JobLoggingDB,
    SandboxMetadataDB,
    TaskQueueDB,
)
from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, CheckWMSPolicyCallable

logger = logging.getLogger(__name__)

router = DiracxRouter()


@router.delete("/")
async def remove_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    """Fully remove a list of jobs from the WMS databases.

    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and a status change to Deleted (PATCH /jobs/status) should be used instead for any other purpose.
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)

    return await remove_jobs(
        job_ids,
        config,
        job_db,
        job_logging_db,
        sandbox_metadata_db,
        task_queue_db,
        background_task,
    )


@router.patch("/status")
async def set_job_statuses(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> SetJobStatusReturn:
    await check_permissions(
        action=ActionType.MANAGE, job_db=job_db, job_ids=list(job_update)
    )
    # check that the datetime contains timezone info
    for job_id, status in job_update.items():
        for dt in status:
            if dt.tzinfo is None:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail=f"Timestamp {dt} is not timezone aware for job {job_id}",
                )
    result = await set_job_status_bulk(
        job_update,
        config,
        job_db,
        job_logging_db,
        task_queue_db,
        background_task,
        force=force,
    )
    if not result.success:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=result.model_dump(),
        )

    return result


@router.post("/reschedule")
async def reschedule_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
    reset_jobs: Annotated[bool, Query()] = False,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)

    resched_jobs = await reschedule_jobs_bulk(
        job_ids,
        config,
        job_db,
        job_logging_db,
        task_queue_db,
        background_task,
        reset_counter=reset_jobs,
    )

    if not resched_jobs.get("success", []):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=resched_jobs,
        )

    # TODO: send jobs to OtimizationMind
    #  self.__sendJobsToOptimizationMind(validJobList)

    return resched_jobs
