"""Duplicates from /jobs/status.

In the future, pilots will only have DiracX tokens, so they will eventually won't be using /jobs/status anymore.
"""

from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Depends, HTTPException, status

from diracx.core.exceptions import JobNotFoundError, PilotCantAccessJobError
from diracx.core.models import (
    HeartbeatData,
    JobCommand,
    JobStatusUpdate,
    SetJobStatusReturn,
)
from diracx.logic.jobs.status import add_heartbeat as add_heartbeat_bl
from diracx.logic.jobs.status import get_job_commands as get_job_commands_bl
from diracx.logic.jobs.status import (
    set_job_parameters_or_attributes as set_job_parameters_or_attributes_bl,
)
from diracx.logic.jobs.status import set_job_statuses as set_job_statuses_bl
from diracx.routers.utils.pilots import (
    AuthorizedPilotInfo,
    verify_dirac_pilot_access_token,
    verify_that_pilot_can_access_jobs,
)

from ..dependencies import (
    Config,
    JobDB,
    JobLoggingDB,
    JobParametersDB,
    PilotAgentsDB,
    TaskQueueDB,
)
from ..fastapi_classes import DiracxRouter

router = DiracxRouter()


@router.patch("/status")
async def pilot_set_job_statuses(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    pilot_db: PilotAgentsDB,
    pilot_info: Annotated[
        AuthorizedPilotInfo, Depends(verify_dirac_pilot_access_token)
    ],
    force: bool = False,
) -> SetJobStatusReturn:
    # Endpoint only for DiracX pilots (with a pilot token)
    try:
        await verify_that_pilot_can_access_jobs(
            pilot_db=pilot_db,
            pilot_stamp=pilot_info.pilot_stamp,
            job_ids=list(job_update),
        )
    except (PilotCantAccessJobError, JobNotFoundError) as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Pilot can't access this job."
        ) from e

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
async def pilot_add_heartbeat(
    data: dict[int, HeartbeatData],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    pilot_db: PilotAgentsDB,
    pilot_info: Annotated[
        AuthorizedPilotInfo, Depends(verify_dirac_pilot_access_token)
    ],
) -> list[JobCommand]:
    """Register a heartbeat from the job.

    This endpoint is used by the JobAgent to send heartbeats to the WMS and to
    receive job commands from the WMS. It also results in stalled jobs being
    restored to the RUNNING status.

    The `data` parameter and return value are mappings keyed by job ID.
    """
    # Endpoint only for DiracX pilots (with a pilot token)
    try:
        await verify_that_pilot_can_access_jobs(
            pilot_db=pilot_db,
            pilot_stamp=pilot_info.pilot_stamp,
            job_ids=list(data),
        )
    except (PilotCantAccessJobError, JobNotFoundError) as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Pilot can't access this job."
        ) from e

    await add_heartbeat_bl(
        data, config, job_db, job_logging_db, task_queue_db, job_parameters_db
    )
    return await get_job_commands_bl(data, job_db)


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def pilot_patch_metadata(
    updates: dict[int, dict[str, Any]],
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
    pilot_db: PilotAgentsDB,
    pilot_info: Annotated[
        AuthorizedPilotInfo, Depends(verify_dirac_pilot_access_token)
    ],
):
    # Endpoint only for DiracX pilots (with a pilot token)
    try:
        await verify_that_pilot_can_access_jobs(
            pilot_db=pilot_db,
            pilot_stamp=pilot_info.pilot_stamp,
            job_ids=list(updates),
        )
    except (PilotCantAccessJobError, JobNotFoundError) as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Pilot can't access this job."
        ) from e

    try:
        await set_job_parameters_or_attributes_bl(updates, job_db, job_parameters_db)
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
