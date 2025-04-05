from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import HTTPException, Query

from diracx.core.models import (
    HeartbeatData,
    JobCommand,
    JobStatusUpdate,
    SetJobStatusReturn,
)
from diracx.logic.jobs.status import add_heartbeat as add_heartbeat_bl
from diracx.logic.jobs.status import get_job_commands as get_job_commands_bl
from diracx.logic.jobs.status import remove_jobs as remove_jobs_bl
from diracx.logic.jobs.status import reschedule_jobs as reschedule_jobs_bl
from diracx.logic.jobs.status import (
    set_job_parameters_or_attributes as set_job_parameters_or_attributes_bl,
)
from diracx.logic.jobs.status import set_job_statuses as set_job_statuses_bl

from ..dependencies import (
    Config,
    JobDB,
    JobLoggingDB,
    JobParametersDB,
    SandboxMetadataDB,
    TaskQueueDB,
)
from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, CheckWMSPolicyCallable

router = DiracxRouter()


@router.delete("/")
async def remove_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Fully remove a list of jobs from the WMS databases.

    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and a status change to Deleted (PATCH /jobs/status) should be used instead for any other purpose.
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)

    return await remove_jobs_bl(
        job_ids,
        config,
        job_db,
        job_logging_db,
        sandbox_metadata_db,
        task_queue_db,
    )


@router.patch("/status")
async def set_job_statuses(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> SetJobStatusReturn:
    await check_permissions(
        action=ActionType.MANAGE, job_db=job_db, job_ids=list(job_update)
    )

    try:
        result = await set_job_statuses_bl(
            status_changes=job_update,
            config=config,
            job_db=job_db,
            job_logging_db=job_logging_db,
            task_queue_db=task_queue_db,
            job_parameters_db=job_parameters_db,
            force=force,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e

    if not result.success:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=result.model_dump(),
        )

    return result


@router.patch("/heartbeat")
async def add_heartbeat(
    data: dict[int, HeartbeatData],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckWMSPolicyCallable,
) -> list[JobCommand]:
    """Register a heartbeat from the job.

    This endpoint is used by the JobAgent to send heartbeats to the WMS and to
    receive job commands from the WMS. It also results in stalled jobs being
    restored to the RUNNING status.

    The `data` parameter and return value are mappings keyed by job ID.
    """
    await check_permissions(action=ActionType.PILOT, job_db=job_db, job_ids=list(data))

    await add_heartbeat_bl(
        data, config, job_db, job_logging_db, task_queue_db, job_parameters_db
    )
    return await get_job_commands_bl(data, job_db)


@router.post("/reschedule")
async def reschedule_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckWMSPolicyCallable,
    reset_jobs: Annotated[bool, Query()] = False,
) -> dict[str, Any]:
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)

    resched_jobs = await reschedule_jobs_bl(
        job_ids,
        config,
        job_db,
        job_logging_db,
        task_queue_db,
        job_parameters_db,
        reset_jobs=reset_jobs,
    )

    if not resched_jobs.get("success", []):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=resched_jobs,
        )

    # TODO: send jobs to OtimizationMind
    #  self.__sendJobsToOptimizationMind(validJobList)

    return resched_jobs


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def patch_metadata(
    updates: dict[int, dict[str, Any]],
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckWMSPolicyCallable,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=updates)
    try:
        await set_job_parameters_or_attributes_bl(updates, job_db, job_parameters_db)
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
