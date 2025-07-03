from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends

from diracx.core.models import LogLine
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
    lines: Annotated[
        list[LogLine],
        Body(description="Message from the pilot to the logging system.", embed=True),
    ],
    pilot_logs_db: PilotLogsDB,
    pilot_db: PilotAgentsDB,
    pilot_info: Annotated[
        AuthorizedPilotInfo, Depends(verify_dirac_pilot_access_token)
    ],
):
    await send_message_bl(
        lines=lines,
        pilot_logs_db=pilot_logs_db,
        pilot_db=pilot_db,
        vo=pilot_info.vo,
        pilot_stamp=pilot_info.pilot_stamp,
    )
