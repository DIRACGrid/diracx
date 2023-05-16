import asyncio
from typing import Annotated, TypedDict

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from diracx.core.properties import SecurityProperty
from diracx.core.utils import JobStatus
from diracx.db.jobs.db import JobDB, get_job_db

from ..auth import UserInfo, verify_dirac_token
from ..utils import has_properties

MAX_PARAMETRIC_JOBS = 20

router = APIRouter(
    tags=["jobs"],
    dependencies=[
        has_properties(
            SecurityProperty.NORMAL_USER | SecurityProperty.JOB_ADMINISTRATOR
        )
    ],
)


# def get_jobdb():
#     async with sessionmaker() as session:
#         return JobDB(session)


@router.get("/{job_id}")
async def get_single_job(job_id: int):
    return f"This job {job_id}"


@router.delete("/{job_id}")
async def delete_single_job(job_id: int):
    return f"I am deleting {job_id}"


@router.post(
    "/{job_id}/kill", dependencies=[has_properties(SecurityProperty.JOB_ADMINISTRATOR)]
)
async def kill_single_job(job_id: int):
    return f"I am killing {job_id}"


@router.get("/{job_id}/status")
async def get_single_job_status(job_id: int) -> JobStatus:
    return JobStatus.Stalled


@router.post("/{job_id}/status")
async def set_single_job_status(job_id: int, status: JobStatus):
    return f"Updating Job {job_id} to {status}"


@router.get("/")
async def get_bulk_jobs(job_db: Annotated[JobDB, Depends(get_job_db)]) -> list:
    return await job_db.list()


class JobID(BaseModel):
    job_id: int


@router.delete("/")
async def delete_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.post("/kill")
async def kill_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.get("/status")
async def get_bulk_job_status(job_ids: Annotated[list[int], Query(max_items=10)]):
    return [{"job_id": job.job_id, "status": JobStatus.Running} for job in job_ids]


class JobStatusUpdate(BaseModel):
    job_id: int
    status: JobStatus


class JobStatusReturn(TypedDict):
    job_id: int
    status: JobStatus


@router.post("/status")
async def set_status_bulk(job_update: list[JobStatusUpdate]) -> list[JobStatusReturn]:
    return [{"job_id": job.job_id, "status": job.status} for job in job_update]


class JobDefinition(BaseModel):
    owner: str
    group: str
    vo: str
    jdl: str


@router.post("/")
async def submit_bulk_jobs(
    job_definitions: list[str],
    job_db: Annotated[JobDB, Depends(get_job_db)],
    user_info: Annotated[UserInfo, Depends(verify_dirac_token)],
):
    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.Core.Utilities.DErrno import EWMSJDL
    from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import (
        generateParametricJobs,
        getParameterVectorLength,
    )

    fixme_ownerDN = "ownerDN"
    fixme_ownerGroup = "ownerGroup"
    fixme_diracSetup = "diracSetup"

    # TODO: implement actual job policy checking
    # # Check job submission permission
    # result = JobPolicy(
    #     fixme_ownerDN, fixme_ownerGroup, fixme_userProperties
    # ).getJobPolicy()
    # if not result["OK"]:
    #     raise NotImplementedError(EWMSSUBM, "Failed to get job policies")
    # policyDict = result["Value"]
    # if not policyDict[RIGHT_SUBMIT]:
    #     raise NotImplementedError(EWMSSUBM, "Job submission not authorized")

    # TODO make it bulk compatible
    assert len(job_definitions) == 1

    jobDesc = f"[{job_definitions[0]}]"

    # TODO: that needs to go in the legacy adapter
    # # jobDesc is JDL for now
    # jobDesc = jobDesc.strip()
    # if jobDesc[0] != "[":
    #     jobDesc = f"[{jobDesc}"
    # if jobDesc[-1] != "]":
    #     jobDesc = f"{jobDesc}]"

    # Check if the job is a parametric one
    jobClassAd = ClassAd(jobDesc)
    result = getParameterVectorLength(jobClassAd)
    if not result["OK"]:
        print("Issue with getParameterVectorLength", result["Message"])
        return result
    nJobs = result["Value"]
    parametricJob = False
    if nJobs is not None and nJobs > 0:
        # if we are here, then jobDesc was the description of a parametric job. So we start unpacking
        parametricJob = True
        if nJobs > MAX_PARAMETRIC_JOBS:
            raise NotImplementedError(
                EWMSJDL,
                "Number of parametric jobs exceeds the limit of %d"
                % MAX_PARAMETRIC_JOBS,
            )
        result = generateParametricJobs(jobClassAd)
        if not result["OK"]:
            return result
        jobDescList = result["Value"]
    else:
        # if we are here, then jobDesc was the description of a single job.
        jobDescList = [jobDesc]

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
            user_info.sub,
            fixme_ownerDN,
            fixme_ownerGroup,
            fixme_diracSetup,
            initialStatus,
            initialMinorStatus,
            user_info.vo,
        )

        print(
            f'Job added to the JobDB", "{job_id} for {fixme_ownerDN}/{fixme_ownerGroup}'
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
