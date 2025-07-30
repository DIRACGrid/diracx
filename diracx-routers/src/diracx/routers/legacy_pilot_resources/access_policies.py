from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import GENERIC_PILOT, LIMITED_DELEGATION
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class LegacyPilotAccessPolicy(BaseAccessPolicy):
    """Rules:
    * Every user can access data about his VO
    * An administrator can modify a pilot.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
    ):
        if (
            LIMITED_DELEGATION not in user_info.properties
            and GENERIC_PILOT not in user_info.properties
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You must be a pilot to access this resource.",
            )

        return


CheckLegacyPilotPolicyCallable = Annotated[
    Callable, Depends(LegacyPilotAccessPolicy.check)
]
