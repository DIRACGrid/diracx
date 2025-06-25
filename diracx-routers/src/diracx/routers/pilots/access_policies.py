from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import SERVICE_ADMINISTRATOR, TRUSTED_HOST
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    # Change some pilot fields
    MANAGE_PILOTS = auto()
    # Read some pilot info
    READ_PILOT_FIELDS = auto()


class PilotManagementAccessPolicy(BaseAccessPolicy):
    """Rules:
    * Every user can access data about his VO
    * An administrator, as well as a DIRAC service can modify a pilot.
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

        # Users can query
        if action == ActionType.READ_PILOT_FIELDS:
            return

        # If we want to modify pilots, we allow only admins and DIRAC
        if (
            TRUSTED_HOST in user_info.properties
            or SERVICE_ADMINISTRATOR in user_info.properties
        ):
            return

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have the rights to modify a pilot.",
        )


CheckPilotManagementPolicyCallable = Annotated[
    Callable, Depends(PilotManagementAccessPolicy.check)
]
