from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, Response

from diracx.core.models import SearchParams
from diracx.logic.pilots.query import search as search_bl

from ..dependencies import PilotAgentsDB
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()

EXAMPLE_SEARCHES = {
    "Show all": {
        "summary": "Show all",
        "description": "Shows all pilots the current user has access to.",
        "value": {},
    },
    "A specific pilot": {
        "summary": "A specific pilot",
        "description": "Search for a specific pilot by ID",
        "value": {"search": [{"parameter": "PilotID", "operator": "eq", "value": "5"}]},
    },
    "Get ordered pilot statuses": {
        "summary": "Get ordered pilot statuses",
        "description": "Get only pilot statuses for specific pilots, ordered by status",
        "value": {
            "parameters": ["PilotID", "Status"],
            "search": [
                {"parameter": "PilotID", "operator": "in", "values": ["6", "2", "3"]}
            ],
            "sort": [{"parameter": "PilotID", "direction": "asc"}],
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
                        "PilotID": 3,
                        "SubmissionTime": "2023-05-25T07:03:35.602654",
                        "LastUpdateTime": "2023-05-25T07:03:35.602656",
                        "Status": "RUNNING",
                        "GridType": "Dirac",
                        "BenchMark": 1.0,
                    },
                    {
                        "PilotID": 5,
                        "SubmissionTime": "2023-06-25T07:03:35.602654",
                        "LastUpdateTime": "2023-07-25T07:03:35.602652",
                        "Status": "RUNNING",
                        "GridType": "Dirac",
                        "BenchMark": 63.1,
                    },
                ]
            }
        },
    },
    206: {
        "description": "Partial Content. Only a part of the requested range could be served.",
        "headers": {
            "Content-Range": {
                "description": "The range of pilots returned in this response",
                "schema": {"type": "string", "example": "pilots 0-1/4"},
            }
        },
        "model": list[dict[str, Any]],
        "content": {
            "application/json": {
                "example": [
                    {
                        "PilotID": 3,
                        "SubmissionTime": "2023-05-25T07:03:35.602654",
                        "LastUpdateTime": "2023-05-25T07:03:35.602656",
                        "Status": "RUNNING",
                        "GridType": "Dirac",
                        "BenchMark": 1.0,
                    },
                    {
                        "PilotID": 5,
                        "SubmissionTime": "2023-06-25T07:03:35.602654",
                        "LastUpdateTime": "2023-07-25T07:03:35.602652",
                        "Status": "RUNNING",
                        "GridType": "Dirac",
                        "BenchMark": 63.1,
                    },
                ]
            }
        },
    },
}


@router.post("/search", responses=EXAMPLE_RESPONSES)
async def search(
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    page: int = 1,
    per_page: int = 100,
    body: Annotated[
        SearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Retrieve information about pilots."""
    # Inspired by /api/jobs/query
    await check_permissions(action=ActionType.READ_PILOT_FIELDS)

    user_vo = user_info.vo

    total, pilots = await search_bl(
        pilot_db=pilot_db,
        user_vo=user_vo,
        page=page,
        per_page=per_page,
        body=body,
    )

    # Set the Content-Range header if needed
    # https://datatracker.ietf.org/doc/html/rfc7233#section-4

    # No pilots found but there are pilots for the requested search
    # https://datatracker.ietf.org/doc/html/rfc7233#section-4.4
    if len(pilots) == 0 and total > 0:
        response.headers["Content-Range"] = f"pilots */{total}"
        response.status_code = HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE

    # The total number of pilots is greater than the number of pilots returned
    # https://datatracker.ietf.org/doc/html/rfc7233#section-4.2
    elif len(pilots) < total:
        first_idx = per_page * (page - 1)
        last_idx = min(first_idx + len(pilots), total) - 1 if total > 0 else 0
        response.headers["Content-Range"] = f"pilots {first_idx}-{last_idx}/{total}"
        response.status_code = HTTPStatus.PARTIAL_CONTENT
    return pilots
