"""Expose endpoints for updating job status, heartbeat, and metadata.

The routes allow authorised users and pilots to report job status, send
heartbeats, reschedule jobs, and update job attributes or parameters.
"""

from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, HTTPException, Query

from diracx.core.exceptions import JobNotFoundError
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
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> SetJobStatusReturn:
    """Apply status updates to one or more jobs.

    Each job ID maps to one or more timestamped updates. The update fields are
    ``Status``, ``MinorStatus``, ``ApplicationStatus``, and ``Source``; the
    source defaults to ``"Unknown"``. Set ``force`` to override normal status
    transition checks.

    Jobs that cannot be updated are listed under ``failed`` in the response. The
    request fails with 404 only when no job could be updated.

    \f

    Args:
        job_update: Timestamped status changes keyed by job ID.
        config: DiracX configuration used when applying status transitions.
        job_db: Database storing current job attributes.
        job_logging_db: Database storing job status history.
        task_queue_db: Database used to update task-queue state when required.
        check_permissions: Access-policy dependency used to authorize job management.
        force: Whether to bypass normal status transition checks.

    Returns:
        Per-job status-update successes and failures.

    Raises:
        HTTPException: If an update is invalid (400), no job could be updated
            (404), or the user cannot manage the requested jobs (403).
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
    """Record heartbeat data from jobs and return pending commands.

    The JobAgent uses this endpoint to report runtime data and retrieve WMS
    commands. A heartbeat can restore a matched or stalled job to ``RUNNING``.
    The request body is a mapping keyed by job ID.

    \f

    Args:
        data: Heartbeat fields keyed by job ID.
        config: DiracX configuration used when restoring job status.
        job_db: Database used to update job attributes and status.
        job_logging_db: Database used to record status changes.
        task_queue_db: Database used when status changes affect task queues.
        job_parameters_db: Database used to store heartbeat job parameters.
        check_permissions: Access-policy dependency used to authorize pilot actions.

    Returns:
        Pending commands for the reported jobs.

    Raises:
        HTTPException: If any requested job does not exist (404) or the caller
            is not permitted to perform the pilot action (403).
    """
    await check_permissions(action=ActionType.PILOT, job_db=job_db, job_ids=list(data))

    try:
        await add_heartbeat_bl(
            data, config, job_db, job_logging_db, task_queue_db, job_parameters_db
        )
    except JobNotFoundError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
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
    check_permissions: CheckWMSPolicyCallable,
    reset_jobs: Annotated[bool, Query()] = False,
) -> dict[str, Any]:
    """Reschedule jobs so that they are matched and run again.

    The request body contains ``job_ids``, the list of jobs to reschedule.
    Set ``reset_jobs`` to reset each job's reschedule counter instead of
    incrementing it. Jobs that are unverified or exceed their configured
    reschedule limit are reported as failures.

    \f

    Args:
        job_ids: Identifiers of the jobs to reschedule.
        config: DiracX configuration containing per-VO scheduling limits.
        job_db: Database used to retrieve and update job records.
        job_logging_db: Database used to record rescheduling status changes.
        task_queue_db: Database used to update task queues.
        check_permissions: Access-policy dependency used to authorize job management.
        reset_jobs: Whether to reset, rather than increment, reschedule counters.

    Returns:
        Per-job results describing successful and failed rescheduling operations.

    Raises:
        HTTPException: If no requested job can be rescheduled (400) or the
            user cannot manage the requested jobs (403).
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)

    resched_jobs = await reschedule_jobs_bl(
        job_ids,
        config,
        job_db,
        job_logging_db,
        task_queue_db,
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
    """Update job attributes and parameters.

    The request body maps job IDs to the attributes or parameters to update
    (such as ``UserPriority`` or ``HeartBeatTime``); any field of a job
    except its ID may be set. Fields matching known job attributes are
    updated on the job record; other fields are stored as job parameters.

    \f

    Args:
        updates: Metadata updates keyed by job ID.
        job_db: Database used to update job attributes.
        job_parameters_db: Database used to update job parameters.
        check_permissions: Access-policy dependency used to authorize job management.

    Raises:
        HTTPException: If an update is invalid (400), a job is not found (404),
            or the user cannot manage the requested jobs (403).
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=updates)
    try:
        await set_job_parameters_or_attributes_bl(updates, job_db, job_parameters_db)
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    except JobNotFoundError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
