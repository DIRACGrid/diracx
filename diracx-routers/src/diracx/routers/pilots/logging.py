from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, status
from opensearchpy.exceptions import RequestError

from diracx.core.models import LogMessage, SearchParams
from diracx.logic.pilots.logging import search_logs as search_logs_bl
from diracx.logic.pilots.logging import send_message as send_message_bl

from ..dependencies import PilotAgentsDB, PilotLogsDB
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import ActionType, CheckPilotLogsPolicyCallable

logger = logging.getLogger(__name__)
router = DiracxRouter()


@router.post("/message", status_code=HTTPStatus.NO_CONTENT)
async def send_message(
    data: Annotated[
        LogMessage,
        Body(description="Message from the pilot to the logging system.", embed=False),
    ],
    pilot_logs_db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
    pilot_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotLogsPolicyCallable,
):
    # TODO: Get stamp
    await check_permissions(pilot_db=pilot_agents_db, action=ActionType.CREATE_LOG)

    try:
        # FIXME: No auth here
        await send_message_bl(data, pilot_logs_db, pilot_agents_db, pilot_info.vo)
    except Exception as exc:
        # TODO: Remove this.........
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/logs")
async def search_logs(
    pilot_logs_db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotLogsPolicyCallable,
    page: int = 1,
    per_page: int = 100,
    body: Annotated[SearchParams | None, Body(description="Query to execute.")] = None,
) -> list[dict]:
    # users will only see logs from their own VO if enforced by a policy:
    await check_permissions(
        action=ActionType.QUERY_LOGS,
        pilot_db=pilot_agents_db,
    )

    try:
        return await search_logs_bl(
            vo=user_info.vo,
            body=body,
            per_page=per_page,
            page=page,
            pilot_logs_db=pilot_logs_db,
        )
    except RequestError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Bad query."
        ) from e
