from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import (
    SecurityProperty,
    UnevaluatedProperty,
)
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token


def has_properties(expression: UnevaluatedProperty | SecurityProperty):
    """Check if the user has the given properties."""
    evaluator = (
        expression
        if isinstance(expression, UnevaluatedProperty)
        else UnevaluatedProperty(expression)
    )

    async def require_property(
        user: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    ):
        if not evaluator(user.properties):
            raise HTTPException(status.HTTP_403_FORBIDDEN)

    return Depends(require_property)
