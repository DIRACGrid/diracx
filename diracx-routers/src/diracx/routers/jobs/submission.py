"""Provide an endpoint for submitting jobs described in JDL format.

Submitted jobs are associated with the authenticated user's VO and group and
are returned with their initial status information.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException
from pydantic import BaseModel

from diracx.core.models import InsertedJob
from diracx.db.sql import JobDB, JobLoggingDB
from diracx.logic.jobs import submit_jdl_jobs as submit_jdl_jobs_bl
from diracx.routers.dependencies import Config

from ..fastapi_classes import DiracxRouter
from ..utils import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable

router = DiracxRouter()


class JobID(BaseModel):
    """Identify a job by its database-assigned ID."""

    job_id: int


MAX_PARAMETRIC_JOBS = 20

EXAMPLE_JDLS = {
    "Simple JDL": {
        "value": [
            """Arguments = "jobDescription.xml -o LogLevel=INFO";
Executable = "dirac-jobexec";
JobGroup = jobGroup;
JobName = jobName;
JobType = User;
LogLevel = INFO;
OutputSandbox =
    {
        Script1_CodeOutput.log,
        std.err,
        std.out
    };
Priority = 1;
Site = ANY;
StdError = std.err;
StdOutput = std.out;"""
        ]
    },
    "Parametric JDL": {
        "value": ["""Arguments = "jobDescription.xml -o LogLevel=INFO"""]
    },
}


@router.post("/jdl", status_code=HTTPStatus.CREATED)
async def submit_jdl_jobs(
    job_definitions: Annotated[list[str], Body(openapi_examples=EXAMPLE_JDLS)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
    config: Config,
) -> list[InsertedJob]:
    """Submit one or more jobs described in JDL format.

    A single parametric JDL may expand into multiple jobs. Bulk submissions
    cannot contain parametric jobs, and a submission may not create more jobs
    than the server's per-submission limit; the error response states the limit.

    \f

    Args:
        job_definitions: JDL descriptions for the jobs to submit.
        job_db: Database used to create job records.
        job_logging_db: Database used to record each job's initial status.
        user_info: Authenticated user, VO, and group used as the job owner.
        check_permissions: Access-policy dependency used to authorize job creation.
        config: DiracX configuration used to validate and prepare the JDLs.

    Returns:
        Details of the inserted jobs, including IDs and initial statuses.

    Raises:
        HTTPException: If a JDL is invalid or the submission exceeds the job
            limit (400), or the user is not permitted to create jobs (403).
    """
    await check_permissions(action=ActionType.CREATE, job_db=job_db)

    try:
        inserted_jobs = await submit_jdl_jobs_bl(
            job_definitions, job_db, job_logging_db, user_info, config
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    return inserted_jobs
