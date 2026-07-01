"""Job status router endpoints for DIRACX.

This module exposes HTTP endpoints for updating job statuses in DIRACX.
It delegates the status change business logic to the logic layer and handles
permission checks, validation, and HTTP error translation.
"""

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
from diracx.db.os import JobParametersDB
from diracx.db.sql import JobDB, JobLoggingDB, TaskQueueDB
from diracx.logic.jobs import add_heartbeat as add_heartbeat_bl
from diracx.logic.jobs import get_job_commands as get_job_commands_bl
from diracx.logic.jobs import reschedule_jobs as reschedule_jobs_bl
from diracx.logic.jobs import (
    set_job_parameters_or_attributes as set_job_parameters_or_attributes_bl,
)
from diracx.logic.jobs import set_job_statuses as set_job_statuses_bl
from diracx.routers.dependencies import Config

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
    """Set job status updates for one or more jobs.

    Accepts a mapping of job IDs to timestamped status update payloads. Each
    update is applied via the business logic layer and resulting actions
    (logging, task queueing, parameter updates) are performed as needed.

    Args:
        job_update (dict[int, dict[datetime, JobStatusUpdate]]): Mapping from
            job ID to a mapping of timestamp to status update payload. Each
            JobStatusUpdate may include the following keys:
            - ``Status``: The new status of the job.
            - ``MinorStatus``: A more detailed status description.
            - ``ApplicationStatus``: Application-specific status information.
            - ``Source``: The source of the status update (defaults to
              ``Unknown`` if omitted).
            See ``EXAMPLE_STATUS_UPDATES`` for request examples.
        config (Config): Application configuration.
        job_db (JobDB): Database access object for jobs.
        job_logging_db (JobLoggingDB): Database access for job logs.
        task_queue_db (TaskQueueDB): Task queue database used to enqueue
            follow-up work.
        job_parameters_db (JobParametersDB): Database for job parameters and
            attributes.
        check_permissions (CheckWMSPolicyCallable): Callable to verify the
            caller may manage the affected jobs.
        force (bool, optional): When True, force status changes even if they
            would normally be rejected. Defaults to False.

    Returns:
        SetJobStatusReturn: Result object summarizing the outcome of the
            requested status updates.

    Raises:
        HTTPException: If validation fails or the update cannot be applied
            (HTTP 400), or if a referenced job cannot be found (HTTP 404).
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
            detail=result.model_dump(by_alias=True),
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
    """Register heartbeat data and return job commands.

    The JobAgent calls this endpoint to report runtime metrics and retrieve
    pending commands. The request and response are mappings keyed by job ID.

    Args:
        data (dict[int, HeartbeatData]): Mapping from job ID to heartbeat
            metrics. See ``EXAMPLE_HEARTBEAT`` for example payloads.
        config (Config): Application configuration.
        job_db (JobDB): Database access object for jobs.
        job_logging_db (JobLoggingDB): Database access for job logs.
        task_queue_db (TaskQueueDB): Task queue database used to enqueue
            follow-up work.
        job_parameters_db (JobParametersDB): Database for job parameters.
        check_permissions (CheckWMSPolicyCallable): Callable to verify the
            caller has pilot/manage privileges for the supplied job IDs.

    Returns:
        list[JobCommand]: A list of commands the agent should execute for the
            supplied jobs.

    Raises:
        HTTPException: If permission checks fail.
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
    """Reschedule killed or failed jobs.

    Args:
        job_ids (list[int]): List of job identifiers to reschedule. Examples
            are provided via ``EXAMPLE_RESCHEDULE`` in the module.
        config (Config): Application configuration.
        job_db (JobDB): Database access object for jobs.
        job_logging_db (JobLoggingDB): Database access for job logs.
        task_queue_db (TaskQueueDB): Task queue database used to enqueue
            rescheduled jobs.
        job_parameters_db (JobParametersDB): Database for job parameters.
        check_permissions (CheckWMSPolicyCallable): Callable to verify the
            caller may manage the specified jobs.
        reset_jobs (bool, optional): If True, reset the reschedule counters
            for the jobs. Defaults to False.

    Returns:
        dict[str, Any]: Result of the reschedule operation, including a
            ``success`` list of rescheduled job IDs when successful.

    Raises:
        HTTPException: If the reschedule operation fails (HTTP 400).
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
    """Patch job metadata attributes for multiple jobs.

    The request body is a mapping from job ID to a dictionary of metadata
    attributes to update (e.g. ``UserPriority``, ``HeartBeatTime``,
    ``JobType``). See ``EXAMPLE_METADATA`` for example payloads.

    Args:
        updates (dict[int, JobMetaData]): Mapping from job ID to metadata
            updates to apply.
        job_db (JobDB): Database access object for jobs.
        job_parameters_db (JobParametersDB): Database for job parameters.
        check_permissions (CheckWMSPolicyCallable): Callable to verify the
            caller may manage the specified jobs.

    Returns:
        None

    Raises:
        HTTPException: If validation fails or updates cannot be applied
            (HTTP 400).
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=updates)
    try:
        await set_job_parameters_or_attributes_bl(updates, job_db, job_parameters_db)
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
