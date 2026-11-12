"""Define the access policy of the Transformation System endpoints.

Submitting a workgraph creates productions on behalf of the VO, so it is
restricted to production managers. The policy is provisional: the
Transformation System does not yet say which other identities may submit,
for instance users submitting their own analysis productions.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from http import HTTPStatus
from typing import Annotated

from fastapi import Depends, HTTPException

from diracx.core.properties import PRODUCTION_MANAGEMENT
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    """Operations protected by the transformation access policy."""

    # Submit a workgraph and create its transformations
    SUBMIT_WORKGRAPH = auto()


class TransformationAccessPolicy(BaseAccessPolicy):
    """Authorize operations on workgraphs and transformations."""

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
    ):
        """Check whether the caller may perform the requested operation.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated identity and security properties.
            action: Operation to authorize.

        Raises:
            ValueError: If the required action is not provided.
            HTTPException: With status 403 if the caller lacks permission.
        """
        if action is None:
            raise ValueError("action is a mandatory parameter")

        if action == ActionType.SUBMIT_WORKGRAPH:
            if PRODUCTION_MANAGEMENT not in user_info.properties:
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail="Submitting a workgraph requires ProductionManagement.",
                )
            return

        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail=f"Unknown action {action}"
        )


CheckTransformationPolicyCallable = Annotated[
    Callable, Depends(TransformationAccessPolicy.check)
]
