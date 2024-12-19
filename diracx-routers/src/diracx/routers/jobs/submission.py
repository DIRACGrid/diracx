from __future__ import annotations

import logging
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException
from pydantic import BaseModel
from typing_extensions import TypedDict

from diracx.core.models import (
    JobStatus,
)
from diracx.db.sql.job_logging.db import JobLoggingRecord
from diracx.db.sql.utils.job import JobSubmissionSpec, submit_jobs_jdl

from ..dependencies import (
    JobDB,
    JobLoggingDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable

logger = logging.getLogger(__name__)

router = DiracxRouter()


class InsertedJob(TypedDict):
    JobID: int
    Status: str
    MinorStatus: str
    TimeStamp: datetime


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
async def submit_bulk_jdl_jobs(
    job_definitions: Annotated[list[str], Body(openapi_examples=EXAMPLE_JDLS)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
) -> list[InsertedJob]:

    await check_permissions(action=ActionType.CREATE, job_db=job_db)

    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import (
        generateParametricJobs,
        getParameterVectorLength,
    )

    # TODO: that needs to go in the legacy adapter (Does it ? Because bulk submission is not supported there)
    for i in range(len(job_definitions)):
        job_definition = job_definitions[i].strip()
        if not (job_definition.startswith("[") and job_definition.endswith("]")):
            job_definition = f"[{job_definition}]"
        job_definitions[i] = job_definition

    if len(job_definitions) == 1:
        # Check if the job is a parametric one
        job_class_ad = ClassAd(job_definitions[0])
        result = getParameterVectorLength(job_class_ad)
        if not result["OK"]:
            # FIXME dont do this
            print("Issue with getParameterVectorLength", result["Message"])
            return result
        n_jobs = result["Value"]
        parametric_job = False
        if n_jobs is not None and n_jobs > 0:
            # if we are here, then jobDesc was the description of a parametric job. So we start unpacking
            parametric_job = True
            result = generateParametricJobs(job_class_ad)
            if not result["OK"]:
                # FIXME why?
                return result
            job_desc_list = result["Value"]
        else:
            # if we are here, then jobDesc was the description of a single job.
            job_desc_list = job_definitions
    else:
        # if we are here, then jobDesc is a list of JDLs
        # we need to check that none of them is a parametric
        for job_definition in job_definitions:
            res = getParameterVectorLength(ClassAd(job_definition))
            if not res["OK"]:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST, detail=res["Message"]
                )
            if res["Value"]:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail="You cannot submit parametric jobs in a bulk fashion",
                )

        job_desc_list = job_definitions
        # parametric_job = True
        parametric_job = False

    # TODO: make the max number of jobs configurable in the CS
    if len(job_desc_list) > MAX_PARAMETRIC_JOBS:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Normal user cannot submit more than {MAX_PARAMETRIC_JOBS} jobs at once",
        )

    result = []

    if parametric_job:
        initial_status = JobStatus.SUBMITTING
        initial_minor_status = "Bulk transaction confirmation"
    else:
        initial_status = JobStatus.RECEIVED
        initial_minor_status = "Job accepted"

    submitted_job_ids = await submit_jobs_jdl(
        [
            JobSubmissionSpec(
                jdl=jdl,
                owner=user_info.preferred_username,
                owner_group=user_info.dirac_group,
                initial_status=initial_status,
                initial_minor_status=initial_minor_status,
                vo=user_info.vo,
            )
            for jdl in job_desc_list
        ],
        job_db=job_db,
    )

    logging.debug(
        f'Jobs added to the JobDB", "{submitted_job_ids} for {user_info.preferred_username}/{user_info.dirac_group}'
    )

    job_created_time = datetime.now(timezone.utc)
    await job_logging_db.bulk_insert_record(
        [
            JobLoggingRecord(
                job_id=int(job_id),
                status=initial_status,
                minor_status=initial_minor_status,
                application_status="Unknown",
                date=job_created_time,
                source="JobManager",
            )
            for job_id in submitted_job_ids
        ]
    )

    # if not parametric_job:
    #     self.__sendJobsToOptimizationMind(submitted_job_ids)

    return [
        InsertedJob(
            JobID=job_id,
            Status=initial_status,
            MinorStatus=initial_minor_status,
            TimeStamp=job_created_time,
        )
        for job_id in submitted_job_ids
    ]
