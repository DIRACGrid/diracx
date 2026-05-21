from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class RSSAccessPolicy(BaseAccessPolicy):
    """Any authenticated user can access."""

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
    ):
        if user_info.preferred_username:
            return

        raise HTTPException(status.HTTP_403_FORBIDDEN)


CheckRSSPolicyCallable = Annotated[Callable, Depends(RSSAccessPolicy.check)]
