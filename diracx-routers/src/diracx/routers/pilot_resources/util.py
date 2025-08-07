from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Depends

from diracx.core.models import (
    PilotInfo,
)
from diracx.routers.utils.pilots import (
    AuthorizedPilotInfo,
    verify_dirac_pilot_access_token,
)

from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False)

logger = logging.getLogger(__name__)


@router.get("/pilotinfo")
async def userinfo(
    pilot_info: Annotated[
        AuthorizedPilotInfo, Depends(verify_dirac_pilot_access_token)
    ],
) -> PilotInfo:
    """Get information about the user's identity."""
    return PilotInfo(
        sub=pilot_info.sub, vo=pilot_info.vo, pilot_stamp=pilot_info.pilot_stamp
    )
