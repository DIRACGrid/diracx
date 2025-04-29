from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import NORMAL_USER, TRUSTED_HOST
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    # Create a pilot or a secret
    CREATE_PILOT_OR_SECRET = auto()
    # Associate a pilot with a secret
    ASSOCIATE_PILOT_WITH_SECRET = auto()


class PilotCredentialsAccessPolicy(BaseAccessPolicy):
    """Rules:
    * You need either NORMAL_USER or TRUSTED_HOST in your properties
    * A NORMAL_USER can create a secret
    * A NORMAL_USER and TRUSTED_HOST can associate a pilot with a secret.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        vo: str | None = None,
        action: ActionType | None = None,
    ):
        assert action, "action is a mandatory parameter"
        assert vo, "vo is a mandatory parameter"

        if not vo == user_info.vo:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have the right VO for this resource.",
            )

        if action == ActionType.CREATE_PILOT_OR_SECRET:
            if NORMAL_USER in user_info.properties:
                return

            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have the rights to create secrets.",
            )
        if action == ActionType.ASSOCIATE_PILOT_WITH_SECRET:
            if {NORMAL_USER, TRUSTED_HOST} & set(user_info.properties):
                return

            raise HTTPException(status.HTTP_403_FORBIDDEN)

        raise ValueError("Unknown action.")


CheckPilotCredentialsPolicyCallable = Annotated[
    Callable, Depends(PilotCredentialsAccessPolicy.check)
]
