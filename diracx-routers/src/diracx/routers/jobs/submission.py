from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException
from pydantic import BaseModel

from diracx.core.models import InsertedJob
from diracx.logic.jobs.submission import submit_jdl_jobs as submit_jdl_jobs_bl

from ..dependencies import (
    JobDB,
    JobLoggingDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable

router = DiracxRouter()


class JobID(BaseModel):
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
) -> list[InsertedJob]:
    """Submit a list of jobs in JDL format."""
    await check_permissions(action=ActionType.CREATE, job_db=job_db)

    try:
        inserted_jobs = await submit_jdl_jobs_bl(
            job_definitions, job_db, job_logging_db, user_info
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    return inserted_jobs
