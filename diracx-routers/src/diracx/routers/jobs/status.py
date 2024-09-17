from __future__ import annotations

import logging
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import BackgroundTasks, Body, HTTPException, Query
from sqlalchemy.exc import NoResultFound

from diracx.core.exceptions import JobNotFound
from diracx.core.models import (
    JobStatusUpdate,
    SetJobStatusReturn,
)
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.db.sql.utils.job_status import (
    remove_jobs,
    set_job_status,
    set_job_statuses,
)

from ..auth import has_properties
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

router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])


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
    # TODO: Remove once legacy DIRAC no longer needs this

    # TODO: implement job policy
    # Some tests have already been written in the test_job_manager,
    # but they need to be uncommented and are not complete

    await remove_jobs(
        job_ids,
        config,
        job_db,
        job_logging_db,
        sandbox_metadata_db,
        task_queue_db,
        background_task,
    )

    return job_ids


@router.patch("/{job_id}/status")
async def set_single_job_status(
    job_id: int,
    status: Annotated[dict[datetime, JobStatusUpdate], Body()],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> dict[int, SetJobStatusReturn]:
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
    # check that the datetime contains timezone info
    for dt in status:
        if dt.tzinfo is None:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Timestamp {dt} is not timezone aware",
            )

    try:
        latest_status = await set_job_status(
            job_id,
            status,
            config,
            job_db,
            job_logging_db,
            task_queue_db,
            background_task,
            force,
        )
    except JobNotFound as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
    return {job_id: latest_status}


@router.patch("/status")
async def set_job_status_bulk(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> dict[int, SetJobStatusReturn]:
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
    try:
        return await set_job_statuses(
            job_update,
            config,
            job_db,
            job_logging_db,
            task_queue_db,
            background_task,
            force=force,
        )
    except* JobNotFound as group_exc:
        failed_job_ids: list[int] = list({e.job_id for e in group_exc.exceptions})  # type: ignore

        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail={
                "message": f"Failed to set job status on {len(failed_job_ids)} jobs out of {len(job_update)}",
                "valid_job_ids": list(set(job_update) - set(failed_job_ids)),
                "failed_job_ids": failed_job_ids,
            },
        ) from group_exc


# TODO: Add a parameter to replace "resetJob"
@router.post("/reschedule")
async def reschedule_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    check_permissions: CheckWMSPolicyCallable,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)
    rescheduled_jobs = []
    # TODO: Joblist Policy:
    # validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights(
    #        jobList, RIGHT_RESCHEDULE
    #    )
    # For the moment all jobs are valid:
    valid_job_list = job_ids
    for job_id in valid_job_list:
        # TODO: delete job in TaskQueueDB
        # self.taskQueueDB.deleteJob(jobID)
        result = await job_db.rescheduleJob(job_id)
        try:
            res_status = await job_db.get_job_status(job_id)
        except NoResultFound as e:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND, detail=f"Job {job_id} not found"
            ) from e

        initial_status = res_status.Status
        initial_minor_status = res_status.MinorStatus

        await job_logging_db.insert_record(
            int(job_id),
            initial_status,
            initial_minor_status,
            "Unknown",
            datetime.now(timezone.utc),
            "JobManager",
        )
        if result:
            rescheduled_jobs.append(job_id)
    # To uncomment when jobPolicy is setup:
    # if invalid_job_list or non_auth_job_list:
    #     logging.error("Some jobs failed to reschedule")
    #     if invalid_job_list:
    #         logging.info(f"Invalid jobs: {invalid_job_list}")
    #     if non_auth_job_list:
    #         logging.info(f"Non authorized jobs: {nonauthJobList}")

    # TODO: send jobs to OtimizationMind
    #  self.__sendJobsToOptimizationMind(validJobList)
    return rescheduled_jobs


# TODO: Add a parameter to replace "resetJob"
@router.post("/{job_id}/reschedule")
async def reschedule_single_job(
    job_id: int,
    reset_job: Annotated[bool, Query()],
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])

    try:
        result = await job_db.rescheduleJob(job_id, reset_counter=reset_job)
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
    return result


@router.delete("/{job_id}")
async def remove_single_job(
    job_id: int,
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    """Fully remove a job from the WMS databases.

    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and a status change to "Deleted" (PATCH /jobs/{job_id}/status) should be used instead.
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
    # TODO: Remove once legacy DIRAC no longer needs this

    # TODO: implement job policy

    await remove_jobs(
        [job_id],
        config,
        job_db,
        job_logging_db,
        sandbox_metadata_db,
        task_queue_db,
        background_task,
    )

    return f"Job {job_id} has been successfully removed"


@router.patch("/{job_id}")
async def set_single_job_properties(
    job_id: int,
    job_properties: Annotated[dict[str, Any], Body()],
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
    update_timestamp: bool = False,
):
    """Update the given job properties (MinorStatus, ApplicationStatus, etc)."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])

    rowcount = await job_db.set_properties(
        {job_id: job_properties}, update_timestamp=update_timestamp
    )
    if not rowcount:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Job not found")
