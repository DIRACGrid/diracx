"""Lollygag dummy AccessPolicy.

Makes sure we can use a Gubbins-specific property.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from http import HTTPStatus
from typing import Annotated

from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils import AuthorizedUserInfo
from fastapi import Depends, HTTPException

from gubbins.core.properties import GUBBINS_SENSEI


class ActionType(StrEnum):
    """Actions checked by the Lollygag policy."""

    # Create an object
    CREATE = auto()

    # Read an object
    READ = auto()

    # Manage an object; requires the GUBBINS_SENSEI property
    MANAGE = auto()


class LollygagAccessPolicy(BaseAccessPolicy):
    """Authorize Lollygag actions.

    Managing requires the ``GUBBINS_SENSEI`` property.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
    ):
        """Check whether the user may perform a Lollygag action.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated user identity and security properties.
            action: Lollygag action to authorize.

        Raises:
            HTTPException: If the action is ``MANAGE`` and the user does not
                have the ``GUBBINS_SENSEI`` property (403).
        """
        assert action, "action is a mandatory parameter"

        if action == ActionType.MANAGE and GUBBINS_SENSEI not in user_info.properties:
            raise HTTPException(HTTPStatus.FORBIDDEN, detail="Streng verboten !!")


CheckLollygagPolicyCallable = Annotated[Callable, Depends(LollygagAccessPolicy.check)]
