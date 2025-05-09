from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Body, Depends, HTTPException, Query, Response, status

from diracx.core.exceptions import (
    PilotAlreadyExistsError,
    PilotNotFoundError,
)
from diracx.core.models import PilotCredentialsInfo, PilotStampInfo, SearchParams
from diracx.logic.auth.pilot import (
    add_pilot_credentials,
    create_pilot_credentials_response,
    create_stamp_response,
    register_new_pilots,
)
from diracx.logic.pilots.management import (
    clear_pilots_bulk,
    delete_pilots_by_stamps_bulk,
)
from diracx.logic.pilots.management import get_pilot_info as get_pilot_info_bl

from ..dependencies import AuthSettings, PilotAgentsDB
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()

logger = logging.getLogger(__name__)


@router.post("/")
async def add_pilot_stamps(
    pilot_db: PilotAgentsDB,
    pilot_stamps: Annotated[
        list[str],
        Body(description="List of the pilot stamps we want to add to the db."),
    ],
    vo: Annotated[
        str,
        Body(description="Virtual Organisation associated with the inserted pilots."),
    ],
    settings: AuthSettings,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotManagementPolicyCallable,
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    pilot_references: Annotated[
        dict | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
    generate_secrets: Annotated[
        bool, Body(description="Boolean to allow secret creation or not.")
    ] = True,
    pilot_secret_use_count_max: Annotated[
        int, Body(description="Number of times we can use a secret.")
    ] = 1,
) -> list[PilotStampInfo] | list[PilotCredentialsInfo]:
    """Endpoint where a you can create pilots with their references.
    It will return the pilot secrets as well as an expiration date.

    If a pilot stamp already exists, it will block the insertion.
    """
    await check_permissions(action=ActionType.CREATE_PILOT_OR_SECRET, vo=vo)

    try:
        await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            vo=vo,
            grid_type=grid_type,
            pilot_job_references=pilot_references,
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e

    if generate_secrets:
        credentials, expiration_dates = await add_pilot_credentials(
            pilot_stamps=pilot_stamps,
            pilot_db=pilot_db,
            settings=settings,
            vo=vo,
            pilot_secret_use_count_max=pilot_secret_use_count_max,
        )
    # Logs credentials creation
    logger.debug(f"{user_info.preferred_username} added {len(pilot_stamps)} pilots.")

    if generate_secrets:
        return create_pilot_credentials_response(
            pilot_stamps=pilot_stamps,
            pilot_secrets=credentials,
            pilot_expiration_dates=expiration_dates,
        )
    else:
        return create_stamp_response(pilot_stamps=pilot_stamps)


@router.delete("/", status_code=HTTPStatus.NO_CONTENT)
async def delete_pilots(
    pilot_stamps: Annotated[
        list[str], Query(description="Stamps of the pilots we want to delete.")
    ],
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint to delete a pilot."""
    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_stamps=pilot_stamps,
        pilot_db=pilot_agents_db,
    )

    try:
        await delete_pilots_by_stamps_bulk(
            pilot_db=pilot_agents_db, pilot_stamps=pilot_stamps
        )
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one pilot has not been found.",
        ) from e


@router.delete("/interval", status_code=HTTPStatus.NO_CONTENT)
async def clear_pilots(
    pilot_agents_db: PilotAgentsDB,
    age_in_days: Annotated[
        int,
        Query(
            description=(
                "The number of days that define the maximum age of pilots to be deleted."
                "Pilots older than this age will be considered for deletion."
            )
        ),
    ],
    check_permissions: CheckPilotManagementPolicyCallable,
    delete_only_aborted: Annotated[
        bool,
        Query(
            description=(
                "Flag indicating whether to only delete pilots whose status is 'Aborted'."
                "If set to True, only pilots with the 'Aborted' status will be deleted."
                "It is set by default as True to avoid any mistake."
            )
        ),
    ] = True,
):
    """Delete all pilots that lived more than age_in_days."""
    # TODO: Be stricter here and only allow admins?
    # TODO: Add test (how to test? Millisec?)
    await check_permissions(
        action=ActionType.CREATE_PILOT_OR_SECRET,
        pilot_db=pilot_agents_db,
    )

    if age_in_days < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="age_in_days must be positive.",
        )

    await clear_pilots_bulk(
        pilot_db=pilot_agents_db,
        age_in_days=age_in_days,
        delete_only_aborted=delete_only_aborted,
    )


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
        "summary": "Get ordered job statuses",
        "description": "Get only job statuses for specific jobs, ordered by status",
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
async def get_pilot_info(
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    response: Response,
    page: int = 1,
    per_page: int = 100,
    body: Annotated[
        SearchParams | None, Body(openapi_examples=EXAMPLE_SEARCHES)
    ] = None,
) -> list[dict[str, Any]]:
    """Retrieve information about pilots."""
    # TODO: Test this route
    await check_permissions(action=ActionType.READ_PILOT_FIELDS)

    total, pilots = await get_pilot_info_bl(
        pilot_db=pilot_agents_db,
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
