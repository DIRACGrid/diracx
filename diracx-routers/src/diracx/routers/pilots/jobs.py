from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from typing import Any

from fastapi import HTTPException

from diracx.core.models import (
    JobStatusUpdate,
    SetJobStatusReturn,
)
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
from .access_policies import CheckPilotWMSPolicyCallable

router = DiracxRouter()


@router.patch("/status")
async def set_job_statuses(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckPilotWMSPolicyCallable,
    force: bool = False,
) -> SetJobStatusReturn:
    await check_permissions(job_db=job_db, job_ids=list(job_update))

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


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def patch_metadata(
    updates: dict[int, dict[str, Any]],
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
    check_permissions: CheckPilotWMSPolicyCallable,
):
    await check_permissions(job_db=job_db, job_ids=updates)
    try:
        await set_job_parameters_or_attributes_bl(updates, job_db, job_parameters_db)
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
