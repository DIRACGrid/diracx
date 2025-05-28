"""
Lollygag dummy AccessPolicy
Makes sure we can use Gubbins specific property

"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo
from fastapi import Depends, HTTPException, status

from gubbins.core.properties import GUBBINS_SENSEI


class ActionType(StrEnum):
    CREATE = auto()

    READ = auto()

    MANAGE = auto()


class LollygagAccessPolicy(BaseAccessPolicy):
    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
    ):
        assert action, "action is a mandatory parameter"

        if action == ActionType.MANAGE and GUBBINS_SENSEI not in user_info.properties:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Streng verboten !!")


CheckLollygagPolicyCallable = Annotated[Callable, Depends(LollygagAccessPolicy.check)]
