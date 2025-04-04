from __future__ import annotations

from typing import Annotated

from fastapi import (
    Depends,
)

from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import RegisteredPilotAccessPolicyCallable

router = DiracxRouter(require_auth=False)


@router.get("/info")
async def get_pilot_info(
    check_permissions: RegisteredPilotAccessPolicyCallable,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):
    await check_permissions()

    return user_info
