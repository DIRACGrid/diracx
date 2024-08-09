from __future__ import annotations

from enum import StrEnum, auto
from typing import Annotated, Callable

from fastapi import Depends, HTTPException, status

from diracx.core.properties import GENERIC_PILOT, OPERATOR, PILOT, SERVICE_ADMINISTRATOR
from diracx.db.os import PilotLogsDB
from diracx.routers.access_policies import BaseAccessPolicy

from ..utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    #: Create/update pilot log records
    CREATE = auto()
    #: download pilot logs
    READ = auto()
    #: delete pilot logs
    DELETE = auto()
    #: Search
    QUERY = auto()


class PilotLogsAccessPolicy(BaseAccessPolicy):
    """Rules:
    Only PILOT, GENERIC_PILOT, SERVICE_ADMINISTRATOR and OPERATOR can create log records.
    Policies for other actions to be determined.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
        pilot_db: PilotLogsDB | None = None,
        pilot_ids: list[int] | None = None,  # or pilot stamp list ?
    ):
        print("user_info.properties:", user_info.properties)
        assert action, "action is a mandatory parameter"
        assert pilot_db, "pilot_db is a mandatory parameter"

        if GENERIC_PILOT in user_info.properties:
            return
        if PILOT in user_info.properties:
            return
        if SERVICE_ADMINISTRATOR in user_info.properties:
            return
        if OPERATOR in user_info.properties:
            return

        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=user_info.properties)


CheckPilotLogsPolicyCallable = Annotated[Callable, Depends(PilotLogsAccessPolicy.check)]
