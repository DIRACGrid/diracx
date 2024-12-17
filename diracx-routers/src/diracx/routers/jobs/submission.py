from __future__ import annotations

import logging
from asyncio import TaskGroup
from copy import deepcopy
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

from ..dependencies import (
    JobDB,
    JobLoggingDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable

logger = logging.getLogger(__name__)

router = DiracxRouter()


class JobSubmissionSpec(BaseModel):
    jdl: str
    owner: str
    owner_group: str
    initial_status: str
    initial_minor_status: str
    vo: str


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


async def submit_jobs_jdl(jobs: list[JobSubmissionSpec], job_db: JobDB):
    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
        checkAndAddOwner,
        createJDLWithInitialStatus,
    )

    jobs_to_insert = {}
    jdls_to_update = {}
    inputdata_to_insert = {}
    original_jdls = []

    # generate the jobIDs first
    # TODO: should ForgivingTaskGroup be used?
    async with TaskGroup() as tg:
        for job in jobs:
            original_jdl = deepcopy(job.jdl)
            jobManifest = returnValueOrRaise(
                checkAndAddOwner(original_jdl, job.owner, job.owner_group)
            )

            # Fix possible lack of brackets
            if original_jdl.strip()[0] != "[":
                original_jdl = f"[{original_jdl}]"

            original_jdls.append(
                (
                    original_jdl,
                    jobManifest,
                    tg.create_task(job_db.create_job(original_jdl)),
                )
            )

    async with TaskGroup() as tg:
        for job, (original_jdl, jobManifest_, job_id_task) in zip(jobs, original_jdls):
            job_id = job_id_task.result()
            job_attrs = {
                "JobID": job_id,
                "LastUpdateTime": datetime.now(tz=timezone.utc),
                "SubmissionTime": datetime.now(tz=timezone.utc),
                "Owner": job.owner,
                "OwnerGroup": job.owner_group,
                "VO": job.vo,
            }

            jobManifest_.setOption("JobID", job_id)

            # 2.- Check JDL and Prepare DIRAC JDL
            jobJDL = jobManifest_.dumpAsJDL()

            # Replace the JobID placeholder if any
            if jobJDL.find("%j") != -1:
                jobJDL = jobJDL.replace("%j", str(job_id))

            class_ad_job = ClassAd(jobJDL)

            class_ad_req = ClassAd("[]")
            if not class_ad_job.isOK():
                # Rollback the entire transaction
                raise ValueError(f"Error in JDL syntax for job JDL: {original_jdl}")
            # TODO: check if that is actually true
            if class_ad_job.lookupAttribute("Parameters"):
                raise NotImplementedError("Parameters in the JDL are not supported")

            # TODO is this even needed?
            class_ad_job.insertAttributeInt("JobID", job_id)

            await job_db.checkAndPrepareJob(
                job_id,
                class_ad_job,
                class_ad_req,
                job.owner,
                job.owner_group,
                job_attrs,
                job.vo,
            )
            jobJDL = createJDLWithInitialStatus(
                class_ad_job,
                class_ad_req,
                job_db.jdl2DBParameters,
                job_attrs,
                job.initial_status,
                job.initial_minor_status,
                modern=True,
            )

            jobs_to_insert[job_id] = job_attrs
            jdls_to_update[job_id] = jobJDL

            if class_ad_job.lookupAttribute("InputData"):
                inputData = class_ad_job.getListFromExpression("InputData")
                inputdata_to_insert[job_id] = [lfn for lfn in inputData if lfn]

        tg.create_task(job_db.update_job_jdls(jdls_to_update))
        tg.create_task(job_db.insert_job_attributes(jobs_to_insert))

        if inputdata_to_insert:
            tg.create_task(job_db.insert_input_data(inputdata_to_insert))

    return jobs_to_insert.keys()


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
        jobClassAd = ClassAd(job_definitions[0])
        result = getParameterVectorLength(jobClassAd)
        if not result["OK"]:
            # FIXME dont do this
            print("Issue with getParameterVectorLength", result["Message"])
            return result
        nJobs = result["Value"]
        parametricJob = False
        if nJobs is not None and nJobs > 0:
            # if we are here, then jobDesc was the description of a parametric job. So we start unpacking
            parametricJob = True
            result = generateParametricJobs(jobClassAd)
            if not result["OK"]:
                # FIXME why?
                return result
            jobDescList = result["Value"]
        else:
            # if we are here, then jobDesc was the description of a single job.
            jobDescList = job_definitions
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

        jobDescList = job_definitions
        # parametricJob = True
        parametricJob = False

    # TODO: make the max number of jobs configurable in the CS
    if len(jobDescList) > MAX_PARAMETRIC_JOBS:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Normal user cannot submit more than {MAX_PARAMETRIC_JOBS} jobs at once",
        )

    result = []

    if parametricJob:
        initialStatus = JobStatus.SUBMITTING
        initialMinorStatus = "Bulk transaction confirmation"
    else:
        initialStatus = JobStatus.RECEIVED
        initialMinorStatus = "Job accepted"

    submitted_job_ids = await submit_jobs_jdl(
        [
            JobSubmissionSpec(
                jdl=jdl,
                owner=user_info.preferred_username,
                owner_group=user_info.dirac_group,
                initial_status=initialStatus,
                initial_minor_status=initialMinorStatus,
                vo=user_info.vo,
            )
            for jdl in jobDescList
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
                status=initialStatus,
                minor_status=initialMinorStatus,
                application_status="Unknown",
                date=job_created_time,
                source="JobManager",
            )
            for job_id in submitted_job_ids
        ]
    )

    # if not parametricJob:
    #     self.__sendJobsToOptimizationMind(submitted_job_ids)

    return [
        InsertedJob(
            JobID=job_id,
            Status=initialStatus,
            MinorStatus=initialMinorStatus,
            TimeStamp=job_created_time,
        )
        for job_id in submitted_job_ids
    ]
