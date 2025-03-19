from __future__ import annotations

import logging
from enum import StrEnum, auto
from typing import Annotated, Callable

from fastapi import Depends, HTTPException, status

from diracx.core.models import ScalarSearchOperator, ScalarSearchSpec
from diracx.core.properties import (
    NORMAL_USER,
)
from diracx.routers.access_policies import BaseAccessPolicy

from ..dependencies import PilotAgentsDB
from ..utils.users import AuthorizedUserInfo

logger = logging.getLogger(__name__)


class ActionType(StrEnum):
    #: Create/update pilot log records
    CREATE = auto()
    #: Search
    QUERY = auto()


class PilotLogsAccessPolicy(BaseAccessPolicy):
    """Rules:
    Only NORMAL_USER in a correct VO and a diracAdmin VO member can query log records.
    All other actions and users are explicitly denied access.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
        pilot_agents_db: PilotAgentsDB | None = None,
        pilot_id: int | None = None,
    ):
        assert pilot_agents_db
        if action is None:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail="Action is a mandatory argument"
            )
        elif action == ActionType.QUERY:
            if pilot_id is None:
                logger.error("Pilot ID value is not provided (None)")
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    detail=f"PilotID not provided: {pilot_id}",
                )
            search_params = ScalarSearchSpec(
                parameter="PilotID",
                operator=ScalarSearchOperator.EQUAL,
                value=pilot_id,
            )

            total, result = await pilot_agents_db.search(["VO"], [search_params], [])
            # we expect exactly one row.
            if total != 1:
                logger.error(
                    "Cannot determine VO for requested PilotID: %d, found %d candidates.",
                    pilot_id,
                    total,
                )
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST, detail=f"PilotID not found: {pilot_id}"
                )
            vo = result[0]["VO"]

            if user_info.vo == "diracAdmin":
                return

            if NORMAL_USER in user_info.properties and user_info.vo == vo:
                return

            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to access this pilot's log.",
            )
        else:
            raise NotImplementedError(action)


CheckPilotLogsPolicyCallable = Annotated[Callable, Depends(PilotLogsAccessPolicy.check)]
