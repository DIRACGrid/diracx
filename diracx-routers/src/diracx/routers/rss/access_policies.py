"""Define the access policy for RSS resource-status endpoints.

The policy permits any authenticated user to access status resources; endpoint
handlers remain responsible for scoping results to the user's VO.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import Depends

from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class RSSAccessPolicy(BaseAccessPolicy):
    """Allow authenticated users to access RSS resources.

    Authentication is provided by ``verify_dirac_access_token``. VO scoping is
    applied by the RSS route handlers rather than by this policy.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
    ):
        """Allow the authenticated user to proceed with an RSS request.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated user identity and security properties.
        """
        # Authentication is already guaranteed by verify_dirac_access_token;
        # any authenticated user may read resource statuses. VO scoping is
        # applied in the routes themselves.
        return


CheckRSSPolicyCallable = Annotated[Callable, Depends(RSSAccessPolicy.check)]
