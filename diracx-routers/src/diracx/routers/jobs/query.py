from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, Response
from pydantic import BaseModel

from diracx.core.models import (
    ScalarSearchOperator,
    SearchSpec,
    SortSpec,
)

from ..dependencies import (
    Config,
    JobDB,
    JobParametersDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckWMSPolicyCallable

logger = logging.getLogger(__name__)

router = DiracxRouter()


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
