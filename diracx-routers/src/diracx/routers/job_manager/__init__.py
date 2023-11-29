from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Annotated, Any, TypedDict

from fastapi import BackgroundTasks, Body, Depends, HTTPException, Query
from pydantic import BaseModel, root_validator
from sqlalchemy.exc import NoResultFound

from diracx.core.config import Config, ConfigSource
from diracx.core.exceptions import JobNotFound
from diracx.core.models import (
    JobStatus,
    JobStatusReturn,
    JobStatusUpdate,
    LimitedJobStatusReturn,
    ScalarSearchOperator,
    SearchSpec,
    SetJobStatusReturn,
    SortSpec,
)
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.db.sql.jobs.status_utility import (
    delete_jobs,
    kill_jobs,
    remove_jobs,
    set_job_status,
)

from ..auth import AuthorizedUserInfo, has_properties, verify_dirac_access_token
from ..dependencies import JobDB, JobLoggingDB, SandboxMetadataDB, TaskQueueDB
from ..fastapi_classes import DiracxRouter
from .sandboxes import router as sandboxes_router

MAX_PARAMETRIC_JOBS = 20

logger = logging.getLogger(__name__)

router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])
router.include_router(sandboxes_router)


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
    distinct: bool = False

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
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> list[InsertedJob]:
    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_SUBMIT, JobPolicy
    from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import (
        generateParametricJobs,
        getParameterVectorLength,
    )

    class DiracxJobPolicy(JobPolicy):
        def __init__(self, user_info: AuthorizedUserInfo, allInfo: bool = True):
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

    result = []

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
        res = await job_db.insert(
            jobDescription,
            user_info.preferred_username,
            user_info.dirac_group,
            initialStatus,
            initialMinorStatus,
            user_info.vo,
        )

        job_id = res["JobID"]
        logging.debug(
            f'Job added to the JobDB", "{job_id} for {user_info.preferred_username}/{user_info.dirac_group}'
        )

        await job_logging_db.insert_record(
            int(job_id),
            initialStatus,
            initialMinorStatus,
            "Unknown",
            datetime.now(timezone.utc),
            "JobManager",
        )

        result.append(res)

    return result

    # TODO: is this needed ?
    # if not parametricJob:
    #     self.__sendJobsToOptimizationMind(jobIDList)
    # return result

    return await asyncio.gather(
        *(job_db.insert(j.owner, j.group, j.vo) for j in job_definitions)
    )


@router.delete("/")
async def delete_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    # TODO: implement job policy

    try:
        await delete_jobs(
            job_ids,
            config,
            job_db,
            job_logging_db,
            task_queue_db,
            background_task,
        )
    except* JobNotFound as group_exc:
        failed_job_ids: list[int] = list({e.job_id for e in group_exc.exceptions})  # type: ignore

        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail={
                "message": f"Failed to delete {len(failed_job_ids)} jobs out of {len(job_ids)}",
                "valid_job_ids": list(set(job_ids) - set(failed_job_ids)),
                "failed_job_ids": failed_job_ids,
            },
        ) from group_exc

    return job_ids


@router.post("/kill")
async def kill_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    # TODO: implement job policy
    try:
        await kill_jobs(
            job_ids,
            config,
            job_db,
            job_logging_db,
            task_queue_db,
            background_task,
        )
    except* JobNotFound as group_exc:
        failed_job_ids: list[int] = list({e.job_id for e in group_exc.exceptions})  # type: ignore

        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail={
                "message": f"Failed to kill {len(failed_job_ids)} jobs out of {len(job_ids)}",
                "valid_job_ids": list(set(job_ids) - set(failed_job_ids)),
                "failed_job_ids": failed_job_ids,
            },
        ) from group_exc

    return job_ids


@router.post("/remove")
async def remove_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """
    Fully remove a list of jobs from the WMS databases.


    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and the delete endpoint should be used instead for any other purpose.
    """
    # TODO: Remove once legacy DIRAC no longer needs this

    # TODO: implement job policy
    # Some tests have already been written in the test_job_manager,
    # but they need to be uncommented and are not complete

    await remove_jobs(
        job_ids,
        config,
        job_db,
        job_logging_db,
        sandbox_metadata_db,
        task_queue_db,
        background_task,
    )

    return job_ids


@router.get("/status")
async def get_job_status_bulk(
    job_ids: Annotated[list[int], Query()], job_db: JobDB
) -> dict[int, LimitedJobStatusReturn]:
    try:
        result = await asyncio.gather(
            *(job_db.get_job_status(job_id) for job_id in job_ids)
        )
        return {job_id: status for job_id, status in zip(job_ids, result)}
    except JobNotFound as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e


@router.patch("/status")
async def set_job_status_bulk(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    force: bool = False,
) -> dict[int, SetJobStatusReturn]:
    # check that the datetime contains timezone info
    for job_id, status in job_update.items():
        for dt in status:
            if dt.tzinfo is None:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail=f"Timestamp {dt} is not timezone aware for job {job_id}",
                )

    res = await asyncio.gather(
        *(
            set_job_status(job_id, status, job_db, job_logging_db, force)
            for job_id, status in job_update.items()
        )
    )
    return {job_id: status for job_id, status in zip(job_update.keys(), res)}


@router.get("/status/history")
async def get_job_status_history_bulk(
    job_ids: Annotated[list[int], Query()], job_logging_db: JobLoggingDB
) -> dict[int, list[JobStatusReturn]]:
    result = await asyncio.gather(
        *(job_logging_db.get_records(job_id) for job_id in job_ids)
    )
    return {job_id: status for job_id, status in zip(job_ids, result)}


@router.post("/reschedule")
async def reschedule_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):
    rescheduled_jobs = []
    # TODO: Joblist Policy:
    # validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights(
    #        jobList, RIGHT_RESCHEDULE
    #    )
    # For the moment all jobs are valid:
    valid_job_list = job_ids
    for job_id in valid_job_list:
        # TODO: delete job in TaskQueueDB
        # self.taskQueueDB.deleteJob(jobID)
        result = job_db.rescheduleJob(job_id)
        try:
            res_status = await job_db.get_job_status(job_id)
        except NoResultFound as e:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND, detail=f"Job {job_id} not found"
            ) from e

        initial_status = res_status.Status
        initial_minor_status = res_status.MinorStatus

        await job_logging_db.insert_record(
            int(job_id),
            initial_status,
            initial_minor_status,
            "Unknown",
            datetime.now(timezone.utc),
            "JobManager",
        )
        if result:
            rescheduled_jobs.append(job_id)
    # To uncomment when jobPolicy is setup:
    # if invalid_job_list or non_auth_job_list:
    #     logging.error("Some jobs failed to reschedule")
    #     if invalid_job_list:
    #         logging.info(f"Invalid jobs: {invalid_job_list}")
    #     if non_auth_job_list:
    #         logging.info(f"Non authorized jobs: {nonauthJobList}")

    # TODO: send jobs to OtimizationMind
    #  self.__sendJobsToOptimizationMind(validJobList)
    return rescheduled_jobs


@router.post("/{job_id}/reschedule")
async def reschedule_single_job(
    job_id: int,
    job_db: JobDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):
    try:
        result = await job_db.rescheduleJob(job_id)
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
    return result


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
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
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
        body.parameters,
        body.search,
        body.sort,
        distinct=body.distinct,
        page=page,
        per_page=per_page,
    )


@router.post("/summary")
async def summary(
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
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


@router.get("/{job_id}")
async def get_single_job(job_id: int):
    return f"This job {job_id}"


@router.delete("/{job_id}")
async def delete_single_job(
    job_id: int,
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """
    Delete a job by killing and setting the job status to DELETED.
    """

    # TODO: implement job policy
    try:
        await delete_jobs(
            [job_id],
            config,
            job_db,
            job_logging_db,
            task_queue_db,
            background_task,
        )
    except* JobNotFound as e:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND.value, detail=str(e.exceptions[0])
        ) from e

    return f"Job {job_id} has been successfully deleted"


@router.post("/{job_id}/kill")
async def kill_single_job(
    job_id: int,
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """
    Kill a job.
    """

    # TODO: implement job policy

    try:
        await kill_jobs(
            [job_id], config, job_db, job_logging_db, task_queue_db, background_task
        )
    except* JobNotFound as e:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=str(e.exceptions[0])
        ) from e

    return f"Job {job_id} has been successfully killed"


@router.post("/{job_id}/remove")
async def remove_single_job(
    job_id: int,
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """
    Fully remove a job from the WMS databases.

    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and the delete endpoint should be used instead.
    """
    # TODO: Remove once legacy DIRAC no longer needs this

    # TODO: implement job policy

    await remove_jobs(
        [job_id],
        config,
        job_db,
        job_logging_db,
        sandbox_metadata_db,
        task_queue_db,
        background_task,
    )

    return f"Job {job_id} has been successfully removed"


@router.get("/{job_id}/status")
async def get_single_job_status(
    job_id: int, job_db: JobDB
) -> dict[int, LimitedJobStatusReturn]:
    try:
        status = await job_db.get_job_status(job_id)
    except JobNotFound as e:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f"Job {job_id} not found"
        ) from e
    return {job_id: status}


@router.patch("/{job_id}/status")
async def set_single_job_status(
    job_id: int,
    status: Annotated[dict[datetime, JobStatusUpdate], Body()],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    force: bool = False,
) -> dict[int, SetJobStatusReturn]:
    # check that the datetime contains timezone info
    for dt in status:
        if dt.tzinfo is None:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Timestamp {dt} is not timezone aware",
            )

    try:
        latest_status = await set_job_status(
            job_id, status, job_db, job_logging_db, force
        )
    except JobNotFound as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
    return {job_id: latest_status}


@router.get("/{job_id}/status/history")
async def get_single_job_status_history(
    job_id: int,
    job_logging_db: JobLoggingDB,
) -> dict[int, list[JobStatusReturn]]:
    try:
        status = await job_logging_db.get_records(job_id)
    except JobNotFound as e:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="Job not found"
        ) from e
    return {job_id: status}


@router.patch("/{job_id}")
async def set_single_job_properties(
    job_id: int,
    job_properties: Annotated[dict[str, Any], Body()],
    job_db: JobDB,
    update_timestamp: bool = False,
):
    """
    Update the given job properties (MinorStatus, ApplicationStatus, etc)
    """

    rowcount = await job_db.set_properties(
        {job_id: job_properties}, update_timestamp=update_timestamp
    )
    if not rowcount:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Job not found")
