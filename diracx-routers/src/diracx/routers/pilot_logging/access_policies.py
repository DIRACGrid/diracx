from __future__ import annotations

from enum import StrEnum, auto
from typing import Annotated, Callable

from fastapi import Depends, HTTPException, status

from diracx.core.properties import (
    GENERIC_PILOT,
    NORMAL_USER,
    OPERATOR,
    PILOT,
    SERVICE_ADMINISTRATOR,
)
from diracx.routers.access_policies import BaseAccessPolicy

from ..utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    #: Create/update pilot log records
    CREATE = auto()
    #: delete pilot logs
    DELETE = auto()
    #: Search
    QUERY = auto()


class PilotLogsAccessPolicy(BaseAccessPolicy):
    """Rules:
    Only PILOT, GENERIC_PILOT, SERVICE_ADMINISTRATOR and OPERATOR can process log records.
    Policies for other actions to be determined.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
    ):

        assert action, "action is a mandatory parameter"

        if GENERIC_PILOT in user_info.properties and action == ActionType.CREATE:
            return user_info
        if PILOT in user_info.properties and action == ActionType.CREATE:
            return user_info
        if NORMAL_USER in user_info.properties and action == ActionType.QUERY:
            return user_info
        if SERVICE_ADMINISTRATOR in user_info.properties:
            return user_info
        if OPERATOR in user_info.properties:
            return user_info

        raise HTTPException(status.HTTP_403_FORBIDDEN, detail=user_info.properties)


CheckPilotLogsPolicyCallable = Annotated[Callable, Depends(PilotLogsAccessPolicy.check)]
