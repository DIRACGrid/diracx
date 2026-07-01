"""Job submission router endpoints for DIRACX.

This module exposes HTTP endpoints for submitting JDL-defined jobs to the
DIRACX workload management system. It includes payload models and OpenAPI
examples for job submission requests.
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
    """Model representing a single job identifier payload.

    Attributes:
        job_id (int): The numeric identifier of a job.
    """

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


@router.post("/jdl")
async def submit_jdl_jobs(
    job_definitions: Annotated[list[str], Body(openapi_examples=EXAMPLE_JDLS)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
    config: Config,
) -> list[InsertedJob]:
    """Submit one or more jobs described in JDL format.

    The endpoint accepts a list of JDL job description strings and submits
    them to the WMS via the business logic layer. OpenAPI examples are
    available in the module-level ``EXAMPLE_JDLS`` constant.

    Args:
        job_definitions (list[str]): List of job descriptions in JDL format.
            See ``EXAMPLE_JDLS`` for example payloads.
        job_db (JobDB): Database access object for jobs.
        job_logging_db (JobLoggingDB): Database access object for job logs.
        user_info (AuthorizedUserInfo): Authenticated user information.
        check_permissions (CheckWMSPolicyCallable): Callable to verify the
            caller may create jobs.
        config (Config): Application configuration.

    Returns:
        list[InsertedJob]: List of inserted job records containing assigned
            identifiers and other insertion metadata.

    Raises:
        HTTPException: If validation fails or the business logic raises an
            error while submitting the jobs (returns HTTP 400 with details).
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
