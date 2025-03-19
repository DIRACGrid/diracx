from __future__ import annotations

import logging

from fastapi import HTTPException, status

from diracx.core.models import LogMessage
from diracx.logic.pilots.logging import get_logs as get_logs_bl
from diracx.logic.pilots.logging import send_message as send_message_bl

from ..access_policies import open_access
from ..dependencies import PilotAgentsDB, PilotLogsDB
from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, CheckPilotLogsPolicyCallable

logger = logging.getLogger(__name__)
router = DiracxRouter()


@open_access
@router.post("/")
async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
    # user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> int:

    # await check_permissions(action=ActionType.CREATE, pilot_agent_db, pilot_id)
    try:
        pilot_id = await send_message_bl(data, pilot_logs_db, pilot_agents_db)
    except Exception as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return pilot_id


@router.get("/logs")
async def get_logs(
    pilot_id: int,
    db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
) -> list[dict]:

    logger.debug("Retrieving logs for pilot ID %d", pilot_id)
    # users will only see logs from their own VO if enforced by a policy:
    await check_permissions(
        action=ActionType.QUERY, pilot_agents_db=pilot_agents_db, pilot_id=pilot_id
    )

    return await get_logs_bl(pilot_id, db)
