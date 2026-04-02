from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

import yaml
from fastapi import Body, Depends, HTTPException, UploadFile
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


ALLOWED_CWL_CONTENT_TYPES = {
    "application/x-yaml",
    "text/yaml",
    "text/x-yaml",
    "application/yaml",
    "application/octet-stream",  # common default for file uploads
}


@router.post("/")
async def submit_cwl_jobs(
    workflow: UploadFile,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
    config: Config,
    inputs: list[UploadFile] = [],
) -> list[InsertedJob]:
    """Submit CWL workflow jobs.

    Accepts a CWL workflow file and zero or more input YAML files.
    Each input YAML produces a separate job. If no inputs are provided,
    a single job is created with no input parameters.
    """
    await check_permissions(action=ActionType.CREATE, job_db=job_db)

    # Check MIME types
    if workflow.content_type and workflow.content_type not in ALLOWED_CWL_CONTENT_TYPES:
        raise HTTPException(
            status_code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            detail=f"Workflow file must be YAML, got '{workflow.content_type}'",
        )
    for input_file in inputs:
        if (
            input_file.content_type
            and input_file.content_type not in ALLOWED_CWL_CONTENT_TYPES
        ):
            raise HTTPException(
                status_code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                detail=f"Input file '{input_file.filename}' must be YAML, got '{input_file.content_type}'",
            )

    cwl_yaml = (await workflow.read()).decode()

    # Parse input YAMLs into dicts
    input_params_list: list[dict | None] = []
    if inputs:
        for input_file in inputs:
            content = (await input_file.read()).decode()
            try:
                input_params_list.append(yaml.safe_load(content))
            except yaml.YAMLError as e:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail=f"Invalid input YAML '{input_file.filename}': {e}",
                ) from e
    else:
        input_params_list = [None]

    try:
        inserted_jobs = await submit_cwl_jobs_bl(
            cwl_yaml,
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
