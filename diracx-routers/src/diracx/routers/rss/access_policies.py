from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import Depends

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
        # Authentication is already guaranteed by verify_dirac_access_token;
        # any authenticated user may read resource statuses. VO scoping is
        # applied in the routes themselves.
        return


CheckRSSPolicyCallable = Annotated[Callable, Depends(RSSAccessPolicy.check)]
