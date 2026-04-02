from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException
from pydantic import BaseModel

from diracx.core.models.job import InsertedJob
from diracx.db.sql import JobDB, JobLoggingDB
from diracx.logic.jobs.cwl_submission import submit_cwl_jobs as submit_cwl_jobs_bl
from diracx.logic.jobs.submission import submit_jdl_jobs as submit_jdl_jobs_bl
from diracx.routers.dependencies import Config

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
    config: Config,
) -> list[InsertedJob]:
    """Submit a list of jobs in JDL format."""
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


class CWLJobSubmission(BaseModel):
    """Request body for CWL job submission."""

    workflow: str  # CWL workflow definition as YAML string
    inputs: list[dict] = []  # Per-job input parameters; each dict produces one job


@router.post("/")
async def submit_cwl_jobs(
    body: CWLJobSubmission,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
    config: Config,
) -> list[InsertedJob]:
    """Submit CWL workflow jobs.

    Accepts a CWL workflow definition (YAML string) and zero or more
    input parameter dicts. Each input dict produces a separate job.
    If no inputs are provided, a single job is created with no input parameters.
    """
    await check_permissions(action=ActionType.CREATE, job_db=job_db)

    input_params_list: list[dict | None] = list(body.inputs) if body.inputs else [None]

    try:
        inserted_jobs = await submit_cwl_jobs_bl(
            body.workflow,
            input_params_list,
            job_db=job_db,
            job_logging_db=job_logging_db,
            user_info=user_info,
            config=config,
        )
    except (ValueError, NotImplementedError) as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    return inserted_jobs


@router.get("/workflows/{workflow_id}")
async def get_workflow(
    workflow_id: str,
    job_db: JobDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
) -> dict[str, str]:
    """Retrieve a CWL workflow definition by its content-addressed ID."""
    await check_permissions(action=ActionType.QUERY, job_db=job_db)

    try:
        cwl = await job_db.get_workflow(workflow_id)
    except Exception as e:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=str(e),
        ) from e
    return {"workflow_id": workflow_id, "cwl": cwl}
