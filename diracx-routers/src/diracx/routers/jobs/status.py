from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, HTTPException, Query

from diracx.core.models import (
    HeartbeatData,
    JobCommand,
    JobMetaData,
    JobStatusUpdate,
    SetJobStatusReturn,
)
from diracx.logic.jobs.status import add_heartbeat as add_heartbeat_bl
from diracx.logic.jobs.status import get_job_commands as get_job_commands_bl
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
    TaskQueueDB,
)
from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, CheckWMSPolicyCallable

router = DiracxRouter()

EXAMPLE_STATUS_UPDATES = {
    "Default": {
        "value": {
            1: {
                str(datetime.now(timezone.utc)): {
                    "Status": "Killed",
                    "MinorStatus": "Marked as killed",
                    "ApplicationStatus": "Job was killed by user",
                    "Source": "User",
                }
            },
            2: {
                str(datetime.now(timezone.utc)): {
                    "Status": "Failed",
                    "MinorStatus": "Timeout",
                }
            },
        }
    },
    "Structure of the request body": {
        "value": {
            "<job_id>": {
                "<timestamp>": {
                    "Status": "<status>",
                    "MinorStatus": "<minor_status>",
                    "ApplicationStatus": "<application_status>",
                    "Source": "<source>",
                }
            }
        }
    },
}


@router.patch("/status")
async def set_job_statuses(
    job_update: Annotated[
        dict[int, dict[datetime, JobStatusUpdate]],
        Body(openapi_examples=EXAMPLE_STATUS_UPDATES),
    ],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> SetJobStatusReturn:
    """Set the status of a job or a list of jobs.

    Body parameters:
    - `Status`: The new status of the job.
    - `MinorStatus`: The minor status of the job.
    - `ApplicationStatus`: The application-specific status of the job.
    - `Source`: The source of the status update (default is "Unknown").
    """
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


EXAMPLE_HEARTBEAT = {
    "Default": {
        "value": {
            1: {
                "LoadAverage": 2.5,
                "MemoryUsed": 1024.0,
                "Vsize": 2048.0,
                "AvailableDiskSpace": 500.0,
                "CPUConsumed": 75.0,
                "WallClockTime": 3600.0,
                "StandardOutput": "Job is running smoothly.",
            },
            2: {
                "LoadAverage": 1.0,
                "MemoryUsed": 512.0,
                "Vsize": 1024.0,
                "AvailableDiskSpace": 250.0,
                "CPUConsumed": 50.0,
                "WallClockTime": 1800.0,
                "StandardOutput": "Job is waiting for resources.",
            },
        }
    },
    "Structure of the request body": {
        "value": {
            "<job_id>": {
                "LoadAverage": 2.5,
                "MemoryUsed": 1024.0,
                "Vsize": 2048.0,
                "AvailableDiskSpace": 500.0,
                "CPUConsumed": 75.0,
                "WallClockTime": 3600.0,
                "StandardOutput": "Job is running smoothly.",
            }
        }
    },
}


@router.patch("/heartbeat")
async def add_heartbeat(
    data: Annotated[dict[int, HeartbeatData], Body(openapi_examples=EXAMPLE_HEARTBEAT)],
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


EXAMPLE_RESCHEDULE = {
    "Default": {
        "value": {"job_ids": [1, 2, 3]},
    },
    "One job": {
        "value": {"job_ids": [1]},
    },
}


@router.post(
    "/reschedule",
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"examples": EXAMPLE_RESCHEDULE}},
        }
    },
)
async def reschedule_jobs(
    job_ids: Annotated[list[int], Body(embed=True)],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckWMSPolicyCallable,
    reset_jobs: Annotated[bool, Query()] = False,
) -> dict[str, Any]:
    """Reschedule a list of killed or failed jobs.

    Body parameters:
    - `job_ids`: List of job IDs to reschedule.
    - `reset_jobs`: If True, reset the count of reschedules for the jobs.

    """
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


EXAMPLE_METADATA = {
    "Default": {
        "value": {
            1: {
                "UserPriority": 2,
                "HeartBeatTime": str(datetime.now(timezone.utc)),
                "Status": "Done",
                "Site": "Meyrin",
            },
            2: {
                "UserPriority": 1,
                "HeartBeatTime": str(datetime.now(timezone.utc)),
                "JobType": "AnotherType",
            },
        }
    }
}


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def patch_metadata(
    updates: Annotated[dict[int, JobMetaData], Body(openapi_examples=EXAMPLE_METADATA)],
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Update job metadata such as UserPriority, HeartBeatTime, JobType, etc.
    The argument  are all the attributes/parameters of a job (except the ID).
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=updates)
    try:
        await set_job_parameters_or_attributes_bl(updates, job_db, job_parameters_db)
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
