from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, Response

from diracx.core.models import (
    JobSearchParams,
    JobSummaryParams,
)
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


@router.post("/search", responses=EXAMPLE_RESPONSES)
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
        JobSearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Retrieve information about jobs.

    **TODO: Add more docs**
    """
    await check_permissions(action=ActionType.QUERY, job_db=job_db)

    total, jobs = await search_bl(
        config=config,
        job_db=job_db,
        job_parameters_db=job_parameters_db,
        job_logging_db=job_logging_db,
        preferred_username=user_info.preferred_username,
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

    return await summary_bl(
        config=config,
        job_db=job_db,
        preferred_username=user_info.preferred_username,
        body=body,
    )
