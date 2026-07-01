"""Job query router endpoints for DIRACX.

This module exposes HTTP endpoints for searching jobs and computing job
summaries. It supports paginated job search, content-range handling, and
OpenAPI examples for search and summary payloads.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, Query, Response

from diracx.core.models import (
    SearchParams,
    SummaryParams,
)
from diracx.core.properties import JOB_ADMINISTRATOR
from diracx.db.os import JobParametersDB
from diracx.db.sql import JobDB, JobLoggingDB
from diracx.logic.jobs import MAX_PER_PAGE
from diracx.logic.jobs import search as search_bl
from diracx.logic.jobs import summary as summary_bl
from diracx.routers.dependencies import Config

from ..fastapi_classes import DiracxRouter
from ..utils import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable

router = DiracxRouter()

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
    page: Annotated[int, Query(ge=1)] = 1,
    per_page: Annotated[int, Query(ge=1, le=MAX_PER_PAGE)] = 100,
    body: Annotated[
        SearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Search jobs in the job database.

    Performs a paginated search for jobs accessible to the requesting user. The
    search can filter by job parameters, such as jobID, ID, status, owner, etc.,
    specify which job parameters to return, and sort the results.

    Args:
        config (Config): Application configuration.
        job_db (JobDB): Database access object for jobs.
        job_parameters_db (JobParametersDB): Database access for job parameters.
        job_logging_db (JobLoggingDB): Database access for job logging.
        user_info (AuthorizedUserInfo): Authenticated user information.
        check_permissions (CheckWMSPolicyCallable): Callable used to verify access
            permissions for the requesting user.
        response (Response): FastAPI response object used to set headers and status.
        page (int, optional): Page number (1-indexed). Defaults to 1.
        per_page (int, optional): Number of results per page. Defaults to 100.
        body (SearchParams | None, optional): Search parameters payload. Defaults
            to ``None``. Examples are provided in the module-level
            ``EXAMPLE_SEARCHES`` constant and exposed to OpenAPI via the
            ``Body(openapi_examples=EXAMPLE_SEARCHES)`` declaration.

    Returns:
        list[dict[str, Any]]: List of job dictionaries matching the search.

    Raises:
        HTTPException: If permission checks fail or other HTTP-level errors occur.

    Notes:
        - If fewer jobs are returned than exist, the handler sets the
          ``Content-Range`` header and returns HTTP 206 (Partial Content).
        - If the requested range cannot be satisfied but matching jobs exist,
          the handler sets ``Content-Range`` and returns HTTP 416
          (Requested Range Not Satisfiable).

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
    """Summarize jobs by grouping parameters.

    Produces aggregated results based on the provided grouping parameters.

    Each item in the returned list is a dictionary. Behavior depends on the
    `grouping` value supplied in the request body (see :pyclass:`SummaryParams`):

    - When `body.grouping` is non-empty, each dict contains one key per
      grouping parameter with its value, plus a ``count`` key with the number
      of jobs matching that combination of grouping values.
    - When `body.grouping` is empty (no grouping), each dict contains a full
      job representation (the same fields returned by the ``search`` endpoint)
      with an additional ``count`` key (typically equal to 1 for individual
      jobs).

    Args:
        config (Config): Application configuration.
        job_db (JobDB): Database access object for jobs.
        user_info (AuthorizedUserInfo): Authenticated user information.
        body (SummaryParams): Summary parameters describing the grouping and
            optional search filters. The expected body keys include ``grouping``
            (list of fields to group by) and ``search`` (optional filters).
            See the module-level ``EXAMPLE_SUMMARY`` constant for request
            examples.
        check_permissions (CheckWMSPolicyCallable): Callable used to verify
            access permissions for the requesting user.

    Returns:
        list[dict[str, Any]]: Aggregated results as described above. See the
            module-level ``EXAMPLE_SUMMARY_RESPONSES`` constant for concrete
            examples of the returned structure.

    Raises:
        HTTPException: If permission checks fail or other HTTP-level errors occur.

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
