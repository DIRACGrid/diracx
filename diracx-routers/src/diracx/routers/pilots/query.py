from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, Query, Response

from diracx.core.models.search import SearchParams, SummaryParams
from diracx.core.properties import SERVICE_ADMINISTRATOR
from diracx.db.sql import PilotAgentsDB
from diracx.logic.pilots.query import MAX_PER_PAGE
from diracx.logic.pilots.query import search as search_bl
from diracx.logic.pilots.query import summary as summary_bl

from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()


def _vo_constraint_for(user_info: AuthorizedUserInfo) -> str | None:
    """Return the VO filter to apply for this user, or None for admins."""
    if SERVICE_ADMINISTRATOR in user_info.properties:
        return None
    return user_info.vo


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
    "Pilots that ran a given job": {
        "summary": "Pilots that ran a given job",
        "description": (
            "Find all pilots that have run a specific job. `JobID` is a "
            "pseudo-parameter resolved through `JobToPilotMapping` into a "
            "`PilotID` filter; only `eq` and `in` operators are supported."
        ),
        "value": {"search": [{"parameter": "JobID", "operator": "eq", "value": 42}]},
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
                        "Status": "Running",
                        "GridType": "Dirac",
                        "BenchMark": 1.0,
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
    },
}


@router.post("/search", responses=EXAMPLE_RESPONSES)
async def search(
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    page: Annotated[int, Query(ge=1)] = 1,
    per_page: Annotated[int, Query(ge=1, le=MAX_PER_PAGE)] = 100,
    body: Annotated[
        SearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)  # type: ignore
    ] = None,
) -> list[dict[str, Any]]:
    """Retrieve information about pilots.

    Normal users see only their own VO's pilots. Service administrators see
    pilots from all VOs.

    A `JobID` pseudo-parameter is also accepted in the `search` filter
    list (operators `eq` / `in` only): it is transparently resolved
    through `JobToPilotMapping` into a `PilotID` filter, allowing
    callers to ask "pilots that ran this job" through the same endpoint.
    """
    await check_permissions(action=ActionType.READ_PILOT_METADATA)

    total, pilots = await search_bl(
        pilot_db=pilot_db,
        vo_constraint=_vo_constraint_for(user_info),
        page=page,
        per_page=per_page,
        body=body,
    )

    # RFC 7233 Content-Range handling, matching /api/jobs/search
    if len(pilots) == 0 and total > 0:
        response.headers["Content-Range"] = f"pilots */{total}"
        response.status_code = HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE
    elif len(pilots) < total:
        first_idx = per_page * (page - 1)
        last_idx = min(first_idx + len(pilots), total) - 1 if total > 0 else 0
        response.headers["Content-Range"] = f"pilots {first_idx}-{last_idx}/{total}"
        response.status_code = HTTPStatus.PARTIAL_CONTENT
    return pilots


@router.post("/summary")
async def summary(
    pilot_db: PilotAgentsDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    body: SummaryParams,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Aggregate pilot counts suitable for plotting.

    Normal users see only their own VO's pilots. Service administrators see
    pilots from all VOs.
    """
    await check_permissions(action=ActionType.READ_PILOT_METADATA)

    return await summary_bl(
        pilot_db=pilot_db,
        body=body,
        vo_constraint=_vo_constraint_for(user_info),
    )
