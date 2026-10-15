"""Expose authenticated job search and summary endpoints.

Both endpoints restrict results according to the requesting user's access
policy and VO configuration. Search supports filtering, sorting, pagination,
and selecting returned fields; summary groups matching jobs for aggregation.
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
from diracx.db.sql import JobDB, JobLoggingDB, PilotAgentsDB
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
    "Jobs run on a given pilot": {
        "summary": "Jobs run on a given pilot",
        "description": (
            "Find all jobs that have run on a specific pilot. `PilotStamp` "
            "is a pseudo-parameter resolved through `JobToPilotMapping` "
            "into a `JobID` filter; only `eq` and `in` operators are "
            "supported."
        ),
        "value": {
            "search": [
                {"parameter": "PilotStamp", "operator": "eq", "value": "abc-123"}
            ]
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
    pilot_db: PilotAgentsDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckWMSPolicyCallable,
    response: Response,
    page: Annotated[int, Query(ge=1)] = 1,
    per_page: Annotated[int, Query(ge=1, le=MAX_PER_PAGE)] = 100,
    body: Annotated[
        SearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Search jobs using filters, selected fields, and pagination.

    By default, the endpoint returns all fields for jobs the user can access.
    The request body can limit returned fields, filter results, and specify
    sort order. ``PilotStamp`` is accepted as a pseudo-parameter in filters
    with ``eq`` or ``in`` operators and is resolved to a job-ID filter.

    The response uses ``Content-Range`` when results span multiple pages. It
    returns 206 when only part of the matching range is returned, or 416 when
    the requested page is beyond the available results.

    Args:
        config: DiracX configuration used to determine the VO's job visibility rules.
        job_db: Database used to search job records.
        job_parameters_db: Database used to retrieve job parameters.
        job_logging_db: Database used to retrieve requested job logging information.
        pilot_db: Database used to resolve ``PilotStamp`` filters.
        user_info: Authenticated user identity, VO, and security properties.
        check_permissions: Access-policy dependency used to authorize job queries.
        response: Response object used to set range headers and status codes.
        page: One-based page number to return.
        per_page: Maximum number of jobs to return on this page.
        body: Optional filters, selected fields, sorting, and distinct-result settings.

    Returns:
        Job records matching the query and the user's access permissions.

    Raises:
        HTTPException: If the authenticated user is not permitted to query jobs (403).
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
        pilot_db=pilot_db,
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
    """Group matching jobs and return counts for each group.

    The request body specifies grouping fields and optional search filters.
    Results are scoped to the user's VO and, when global job visibility is
    disabled, to the user's own jobs unless they are a job administrator.

    Args:
        config: DiracX configuration used to determine the VO's job visibility rules.
        job_db: Database used to aggregate job records.
        user_info: Authenticated user identity, VO, and security properties.
        body: Grouping fields and optional search filters.
        check_permissions: Access-policy dependency used to authorize job queries.

    Returns:
        Rows containing each grouping-field combination and its matching job count.

    Raises:
        HTTPException: If the authenticated user is not permitted to query jobs (403).
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
