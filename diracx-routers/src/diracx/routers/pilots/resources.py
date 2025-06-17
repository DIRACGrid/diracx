from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, status

from diracx.core.exceptions import PilotNotFoundError
from diracx.core.models import LogMessage
from diracx.logic.pilots.resources import send_message as send_message_bl
from diracx.routers.utils.pilots import (
    AuthorizedPilotInfo,
    verify_dirac_pilot_access_token,
)

from ..dependencies import PilotAgentsDB, PilotLogsDB
from ..fastapi_classes import DiracxRouter

logger = logging.getLogger(__name__)
router = DiracxRouter()


@router.post("/message", status_code=HTTPStatus.NO_CONTENT)
async def send_message(
    data: Annotated[
        LogMessage,
        Body(description="Message from the pilot to the logging system.", embed=False),
    ],
    pilot_logs_db: PilotLogsDB,
    pilot_db: PilotAgentsDB,
    pilot_info: Annotated[
        AuthorizedPilotInfo, Depends(verify_dirac_pilot_access_token)
    ],
):
    if not data.pilot_stamp == pilot_info.pilot_stamp:
        logger.error(
            f"Pilot with stamp {pilot_info.pilot_stamp} tried to write as {data.pilot_stamp}."
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have the right to right this log.",
        )

    try:
        await send_message_bl(data, pilot_logs_db, pilot_db, pilot_info.vo)
    except PilotNotFoundError as e:
        # If we arrive here, because of "if not data.pilot_stamp == ..."
        # we are that this pilot really existed once (has a token with is name)
        # So if it is not found, there's a problem (so don't raise a 4XX error)
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Pilot is known (has a token) but is not anymore in the db.",
        ) from e
