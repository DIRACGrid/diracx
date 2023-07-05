from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from http import HTTPStatus
from typing import Annotated, Any, TypedDict

from fastapi import Body, Depends, HTTPException, Query
from pydantic import BaseModel, root_validator

from diracx.core.config import Config, ConfigSource
from diracx.core.models import ScalarSearchOperator, SearchSpec, SortSpec
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.core.utils import JobStatus

from ..auth import UserInfo, has_properties, verify_dirac_access_token
from ..dependencies import JobDB
from ..fastapi_classes import DiracxRouter

MAX_PARAMETRIC_JOBS = 20

logger = logging.getLogger(__name__)

router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])


class JobSummaryParams(BaseModel):
    grouping: list[str]
    search: list[SearchSpec] = []

    @root_validator
    def validate_fields(cls, v):
        # TODO
        return v


class JobSearchParams(BaseModel):
    parameters: list[str] | None = None
    search: list[SearchSpec] = []
    sort: list[SortSpec] = []

    @root_validator
    def validate_fields(cls, v):
        # TODO
        return v


class JobDefinition(BaseModel):
    owner: str
    group: str
    vo: str
    jdl: str


class InsertedJob(TypedDict):
    JobID: int
    Status: str
    MinorStatus: str
    TimeStamp: datetime


class JobID(BaseModel):
    job_id: int


class JobStatusUpdate(BaseModel):
    job_id: int
    status: JobStatus


class JobStatusReturn(TypedDict):
    job_id: int
    status: JobStatus


EXAMPLE_JDLS = {
    "Simple JDL": [
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
    ],
    "Parametric JDL": ["""Arguments = "jobDescription.xml -o LogLevel=INFO"""],
}


@router.post("/")
async def submit_bulk_jobs(
    # FIXME: Using mutliple doesn't work with swagger?
    job_definitions: Annotated[list[str], Body(example=EXAMPLE_JDLS["Simple JDL"])],
    job_db: JobDB,
    user_info: Annotated[UserInfo, Depends(verify_dirac_access_token)],
) -> list[InsertedJob]:
    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_SUBMIT, JobPolicy
    from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import (
        generateParametricJobs,
        getParameterVectorLength,
    )

    class DiracxJobPolicy(JobPolicy):
        def __init__(self, user_info: UserInfo, allInfo: bool = True):
            self.userName = user_info.preferred_username
            self.userGroup = user_info.dirac_group
            self.userProperties = user_info.properties
            self.jobDB = None
            self.allInfo = allInfo
            self._permissions: dict[str, bool] = {}
            self._getUserJobPolicy()

    # Check job submission permission
    policyDict = returnValueOrRaise(DiracxJobPolicy(user_info).getJobPolicy())
    if not policyDict[RIGHT_SUBMIT]:
        raise HTTPException(HTTPStatus.FORBIDDEN, "You are not allowed to submit jobs")

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
            print("Issue with getParameterVectorLength", result["Message"])
            return result
        nJobs = result["Value"]
        parametricJob = False
        if nJobs is not None and nJobs > 0:
            # if we are here, then jobDesc was the description of a parametric job. So we start unpacking
            parametricJob = True
            result = generateParametricJobs(jobClassAd)
            if not result["OK"]:
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
        parametricJob = True

    # TODO: make the max number of jobs configurable in the CS
    if len(jobDescList) > MAX_PARAMETRIC_JOBS:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Normal user cannot submit more than {MAX_PARAMETRIC_JOBS} jobs at once",
        )

    jobIDList = []

    if parametricJob:
        initialStatus = JobStatus.SUBMITTING
        initialMinorStatus = "Bulk transaction confirmation"
    else:
        initialStatus = JobStatus.RECEIVED
        initialMinorStatus = "Job accepted"

    for (
        jobDescription
    ) in (
        jobDescList
    ):  # jobDescList because there might be a list generated by a parametric job
        job_id = await job_db.insert(
            jobDescription,
            user_info.preferred_username,
            user_info.dirac_group,
            initialStatus,
            initialMinorStatus,
            user_info.vo,
        )

        logging.debug(
            f'Job added to the JobDB", "{job_id} for {user_info.preferred_username}/{user_info.dirac_group}'
        )

        # TODO comment out for test just now
        # self.jobLoggingDB.addLoggingRecord(
        #     jobID,
        #     result["Status"],
        #     result["MinorStatus"],
        #     date=result["TimeStamp"],
        #     source="JobManager",
        # )

        jobIDList.append(job_id)

    return jobIDList

    # TODO: is this needed ?
    # if not parametricJob:
    #     self.__sendJobsToOptimizationMind(jobIDList)
    # return result

    return await asyncio.gather(
        *(job_db.insert(j.owner, j.group, j.vo) for j in job_definitions)
    )


@router.delete("/")
async def delete_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.get("/{job_id}")
async def get_single_job(job_id: int):
    return f"This job {job_id}"


@router.delete("/{job_id}")
async def delete_single_job(job_id: int):
    return f"I am deleting {job_id}"


@router.post("/{job_id}/kill", dependencies=[has_properties(JOB_ADMINISTRATOR)])
async def kill_single_job(job_id: int):
    return f"I am killing {job_id}"


@router.get("/{job_id}/status")
async def get_single_job_status(job_id: int) -> JobStatus:
    return JobStatus.Stalled


@router.post("/{job_id}/status")
async def set_single_job_status(job_id: int, status: JobStatus):
    return f"Updating Job {job_id} to {status}"


@router.post("/kill")
async def kill_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.get("/status")
async def get_bulk_job_status(job_ids: Annotated[list[int], Query(max_items=10)]):
    return [{"job_id": jid, "status": JobStatus.Running} for jid in job_ids]


@router.post("/status")
async def set_status_bulk(job_update: list[JobStatusUpdate]) -> list[JobStatusReturn]:
    return [{"job_id": job.job_id, "status": job.status} for job in job_update]


EXAMPLE_SEARCHES = {
    "Show all": {
        "summary": "Show all",
        "description": "Shows all jobs the current user has access to.",
        "value": {},
    },
    "A specific job": {
        "summary": "A specific job",
        "description": "Search for a specific job by ID",
        "value": {"search": [{"parameter": "JobID", "operator": "eq", "value": "5"}]},
    },
    "Get ordered job statuses": {
        "summary": "Get ordered job statuses",
        "description": "Get only job statuses for specific jobs, ordered by status",
        "value": {
            "parameters": ["JobID", "Status"],
            "search": [
                {"parameter": "JobID", "operator": "in", "values": ["6", "2", "3"]}
            ],
            "sort": [{"parameter": "JobID", "direction": "asc"}],
        },
    },
}

EXAMPLE_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "List of matching results",
        "content": {
            "application/json": {
                "example": [
                    {
                        "JobID": 1,
                        "JobGroup": "jobGroup",
                        "Owner": "myvo:my_nickname",
                        "SubmissionTime": "2023-05-25T07:03:35.602654",
                        "LastUpdateTime": "2023-05-25T07:03:35.602652",
                        "Status": "RECEIVED",
                        "MinorStatus": "Job accepted",
                        "ApplicationStatus": "Unknown",
                    },
                    {
                        "JobID": 2,
                        "JobGroup": "my_nickname",
                        "Owner": "myvo:cburr",
                        "SubmissionTime": "2023-05-25T07:03:36.256378",
                        "LastUpdateTime": "2023-05-25T07:10:11.974324",
                        "Status": "Done",
                        "MinorStatus": "Application Exited Successfully",
                        "ApplicationStatus": "All events processed",
                    },
                ]
            }
        },
    },
}


@router.post("/search", responses=EXAMPLE_RESPONSES)
async def search(
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    user_info: Annotated[UserInfo, Depends(verify_dirac_access_token)],
    page: int = 0,
    per_page: int = 100,
    body: Annotated[
        JobSearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Retrieve information about jobs.

    **TODO: Add more docs**
    """
    if body is None:
        body = JobSearchParams()
    # TODO: Apply all the job policy stuff properly using user_info
    if not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo:
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                "value": user_info.sub,
            }
        )
    # TODO: Pagination
    return await job_db.search(
        body.parameters, body.search, body.sort, page=page, per_page=per_page
    )


@router.post("/summary")
async def summary(
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    user_info: Annotated[UserInfo, Depends(verify_dirac_access_token)],
    body: JobSummaryParams,
):
    """Show information suitable for plotting"""
    # TODO: Apply all the job policy stuff properly using user_info
    if not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo:
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                "value": user_info.sub,
            }
        )
    return await job_db.summary(body.grouping, body.search)
