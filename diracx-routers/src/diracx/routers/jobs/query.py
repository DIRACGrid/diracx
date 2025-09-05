from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, Response

from diracx.core.models import (
    SearchParams,
    SummaryParams,
)
from diracx.core.properties import JOB_ADMINISTRATOR
from diracx.logic.jobs.query import search as search_bl
from diracx.logic.jobs.query import summary as summary_bl

from ..dependencies import (
    Config,
    JobDB,
    JobLoggingDB,
    JobParametersDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable

router = DiracxRouter()


MAX_PER_PAGE = 10000


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


EXAMPLE_SEARCH_RESPONSES: dict[int | str, dict[str, Any]] = {
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


@router.post("/search", responses=EXAMPLE_SEARCH_RESPONSES)
async def search(
    config: Config,
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
    response: Response,
    page: int = 1,
    per_page: int = 100,
    body: Annotated[
        SearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Creates a search query to the job database. This search can be based on
    different parameters, such as jobID, status, owner, etc.

    **Possibilities**
    - Use `search` to filter jobs based on various parameters (optional).
    - Use `parameters` to specify which job parameters to return (optional).
    - Use `sort` to order the results based on specific parameters (optional).

    By default, the search will return all jobs the user has access to, and all the fields
    of the job will be returned.
    """
    await check_permissions(action=ActionType.QUERY, job_db=job_db)

    preferred_username: str | None = user_info.preferred_username
    if JOB_ADMINISTRATOR in user_info.properties:
        preferred_username = None

    total, jobs = await search_bl(
        config=config,
        job_db=job_db,
        job_parameters_db=job_parameters_db,
        job_logging_db=job_logging_db,
        preferred_username=preferred_username,
        vo=user_info.vo,
        page=page,
        per_page=per_page,
        body=body,
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


EXAMPLE_SUMMARY = {
    "Show all": {
        "summary": "Show all",
        "description": "Shows all jobs the current user has access to.",
        "value": {"grouping": [], "search": []},
    },
    "Group by JobGroup": {
        "summary": "Group the jobs by JobGroup.",
        "description": "We get all the unique JobGroups and the number of jobs in each group.",
        "value": {"grouping": ["JobGroup"]},
    },
    "Group by JobGroup with Search": {
        "summary": "Group the jobs by JobGroup and filter by status.",
        "description": "We get all the unique JobGroups where the status is 'Waiting'. We also get the number of jobs."
        "in each group.",
        "value": {
            "grouping": ["JobGroup"],
            "search": [{"parameter": "Status", "operator": "eq", "value": "Waiting"}],
        },
    },
}

EXAMPLE_SUMMARY_RESPONSES = {
    200: {
        "description": "Results of the request",
        "content": {
            "application/json": {
                "examples": {
                    "without_grouping": {
                        "summary": "Results without grouping parameters",
                        "description": "Shows all jobs when no grouping is specified",
                        "value": [
                            {
                                "JobID": 1,
                                "JobType": "User",
                                "JobGroup": "Group_0",
                                "Site": "Site_48",
                                "JobName": "JobName_1",
                                "Owner": "admin",
                                "OwnerGroup": "admin",
                                "VO": "diracAdmin",
                                "SubmissionTime": "2025-07-15T07:15:57",
                                "RescheduleTime": "null",
                                "LastUpdateTime": "2025-07-15T07:15:57",
                                "StartExecTime": "null",
                                "HeartBeatTime": "null",
                                "EndExecTime": "null",
                                "Status": "Received",
                                "MinorStatus": "Job accepted",
                                "ApplicationStatus": "Unknown",
                                "UserPriority": 5,
                                "RescheduleCounter": 0,
                                "VerifiedFlag": "true",
                                "AccountedFlag": "false",
                                "count": 1,
                            },
                            {
                                "JobID": 2,
                                "JobType": "User",
                                "JobGroup": "Group_0",
                                "Site": "Site_36",
                                "JobName": "JobName_2",
                                "Owner": "admin",
                                "OwnerGroup": "admin",
                                "VO": "diracAdmin",
                                "SubmissionTime": "2025-07-15T07:15:57",
                                "RescheduleTime": "null",
                                "LastUpdateTime": "2025-07-15T07:15:57",
                                "StartExecTime": "null",
                                "HeartBeatTime": "null",
                                "EndExecTime": "null",
                                "Status": "Received",
                                "MinorStatus": "Job accepted",
                                "ApplicationStatus": "Unknown",
                                "UserPriority": 8,
                                "RescheduleCounter": 0,
                                "VerifiedFlag": "true",
                                "AccountedFlag": "false",
                                "count": 1,
                            },
                        ],
                    },
                    "with_grouping": {
                        "summary": "Results with grouping parameters",
                        "description": "Shows grouped results when grouping parameters are specified",
                        "value": [
                            {"JobGroup": "Group_0", "count": 4000},
                            {"JobGroup": "Group_1", "count": 2000},
                            {"JobGroup": "Group_3", "count": 1000},
                            {"JobGroup": "Group_4", "count": 1000},
                            {"JobGroup": "Group_401", "count": 20},
                            {"JobGroup": "Group_402", "count": 20},
                        ],
                    },
                }
            }
        },
    }
}


@router.post("/summary", responses=EXAMPLE_SUMMARY_RESPONSES)
async def summary(
    config: Config,
    job_db: JobDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    body: SummaryParams,
    check_permissions: CheckWMSPolicyCallable,
):
    """Group jobs by a specific list of parameters. Returns an array of n-uplets, where each n-uplet contains the
    values of the grouping parameters and the number of jobs that match those values.

    Body parameters:
    - `grouping`: List of parameters to group the jobs by.
    - `search`: List of search parameters to filter the jobs by (optional).

    """
    await check_permissions(action=ActionType.QUERY, job_db=job_db)

    preferred_username: str | None = user_info.preferred_username
    if JOB_ADMINISTRATOR in user_info.properties:
        preferred_username = None

    return await summary_bl(
        config=config,
        job_db=job_db,
        preferred_username=preferred_username,
        vo=user_info.vo,
        body=body,
    )
