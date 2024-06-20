from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import BackgroundTasks, Body, Depends, HTTPException, Query, Response
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound
from typing_extensions import TypedDict

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
from diracx.db.sql.jobs.status_utility import (
    delete_jobs,
    kill_jobs,
    remove_jobs,
    set_job_status,
)

from ..dependencies import Config, JobDB, JobLoggingDB, SandboxMetadataDB, TaskQueueDB
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable
from .sandboxes import router as sandboxes_router

MAX_PARAMETRIC_JOBS = 20

logger = logging.getLogger(__name__)

router = DiracxRouter()
router.include_router(sandboxes_router)


class JobSummaryParams(BaseModel):
    grouping: list[str]
    search: list[SearchSpec] = []
    # TODO: Add more validation


class JobSearchParams(BaseModel):
    parameters: list[str] | None = None
    search: list[SearchSpec] = []
    sort: list[SortSpec] = []
    distinct: bool = False
    # TODO: Add more validation


class InsertedJob(TypedDict):
    JobID: int
    Status: str
    MinorStatus: str
    TimeStamp: datetime


class JobID(BaseModel):
    job_id: int


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


@router.post("/")
async def submit_bulk_jobs(
    job_definitions: Annotated[list[str], Body(openapi_examples=EXAMPLE_JDLS)],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
) -> list[InsertedJob]:
    await check_permissions(action=ActionType.CREATE, job_db=job_db)

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
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):

    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)
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
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)
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
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    """Fully remove a list of jobs from the WMS databases.

    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and the delete endpoint should be used instead for any other purpose.
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)
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
    job_ids: Annotated[list[int], Query()],
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict[int, LimitedJobStatusReturn]:
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=job_ids)
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
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> dict[int, SetJobStatusReturn]:
    await check_permissions(
        action=ActionType.MANAGE, job_db=job_db, job_ids=list(job_update)
    )
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
    job_ids: Annotated[list[int], Query()],
    job_logging_db: JobLoggingDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict[int, list[JobStatusReturn]]:
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=job_ids)
    result = await asyncio.gather(
        *(job_logging_db.get_records(job_id) for job_id in job_ids)
    )
    return {job_id: status for job_id, status in zip(job_ids, result)}


@router.post("/reschedule")
async def reschedule_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    check_permissions: CheckWMSPolicyCallable,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)
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
        result = await job_db.rescheduleJob(job_id)
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
    check_permissions: CheckWMSPolicyCallable,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
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
    206: {
        "description": "Partial Content. Only a part of the requested range could be served.",
        "headers": {
            "Content-Range": {
                "description": "The range of jobs returned in this response",
                "schema": {"type": "string", "example": "jobs 0-1/4"},
            }
        },
        "model": list[dict[str, Any]],
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

MAX_PER_PAGE = 10000


@router.post("/search", responses=EXAMPLE_RESPONSES)
async def search(
    config: Config,
    job_db: JobDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
    response: Response,
    page: int = 1,
    per_page: int = 100,
    body: Annotated[
        JobSearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Retrieve information about jobs.

    **TODO: Add more docs**
    """
    await check_permissions(action=ActionType.QUERY, job_db=job_db)

    # Apply a limit to per_page to prevent abuse of the API
    if per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

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

    total, jobs = await job_db.search(
        body.parameters,
        body.search,
        body.sort,
        distinct=body.distinct,
        page=page,
        per_page=per_page,
    )
    # Set the Content-Range header if needed
    # https://datatracker.ietf.org/doc/html/rfc7233#section-4

    # No jobs found but there are jobs for the requested search
    # https://datatracker.ietf.org/doc/html/rfc7233#section-4.4
    if len(jobs) == 0 and total > 0:
        response.headers["Content-Range"] = f"jobs */{total}"
        response.status_code = HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE

    # The total number of jobs is greater than the number of jobs returned
    # https://datatracker.ietf.org/doc/html/rfc7233#section-4.2
    elif len(jobs) < total:
        first_idx = per_page * (page - 1)
        last_idx = min(first_idx + len(jobs), total) - 1 if total > 0 else 0
        response.headers["Content-Range"] = f"jobs {first_idx}-{last_idx}/{total}"
        response.status_code = HTTPStatus.PARTIAL_CONTENT
    return jobs


@router.post("/summary")
async def summary(
    config: Config,
    job_db: JobDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    body: JobSummaryParams,
    check_permissions: CheckWMSPolicyCallable,
):
    """Show information suitable for plotting."""
    await check_permissions(action=ActionType.QUERY, job_db=job_db)
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
async def get_single_job(
    job_id: int,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=[job_id])
    return f"This job {job_id}"


@router.delete("/{job_id}")
async def delete_single_job(
    job_id: int,
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    """Delete a job by killing and setting the job status to DELETED."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])

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
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    """Kill a job."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])

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
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    """Fully remove a job from the WMS databases.

    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and the delete endpoint should be used instead.
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
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
    job_id: int,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict[int, LimitedJobStatusReturn]:
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=[job_id])
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
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> dict[int, SetJobStatusReturn]:
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
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
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict[int, list[JobStatusReturn]]:
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=[job_id])
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
    check_permissions: CheckWMSPolicyCallable,
    update_timestamp: bool = False,
):
    """Update the given job properties (MinorStatus, ApplicationStatus, etc)."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])

    rowcount = await job_db.set_properties(
        {job_id: job_properties}, update_timestamp=update_timestamp
    )
    if not rowcount:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Job not found")
