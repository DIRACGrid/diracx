from enum import Enum
from typing import Annotated

from fastapi import Depends, HTTPException, status

from .properties import SecurityProperty, UnevaluatedProperty
from .routers.auth import UserInfo, verify_dirac_token


def has_properties(expression: UnevaluatedProperty | SecurityProperty):
    if not isinstance(expression, UnevaluatedProperty):
        expression = UnevaluatedProperty(expression)

    async def require_property(user: Annotated[UserInfo, Depends(verify_dirac_token)]):
        if not expression(user.properties):
            raise HTTPException(status.HTTP_403_FORBIDDEN)

    return Depends(require_property)


class JobStatus(str, Enum):
    Running = "Running"
    Stalled = "Stalled"
    Killed = "Killed"
    Failed = "Failed"
    RECEIVED = "RECEIVED"
    SUBMITTING = "Submitting"
