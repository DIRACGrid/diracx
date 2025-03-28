from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

# TODO:  DEBUG
from diracx.core.properties import GENERIC_PILOT, LIMITED_DELEGATION
from diracx.db.sql import PilotAgentsDB
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    #: Create a job or a sandbox
    CREATE = auto()
    #: Check job status, download a sandbox
    READ = auto()
    #: delete, kill, remove, set status, etc of a job
    #: delete or assign a sandbox
    MANAGE = auto()
    #: Search
    QUERY = auto()


class RegisteredPilotAccessPolicy(BaseAccessPolicy):

    @staticmethod
    async def policy(
        policy_name: str,
        pilot_info: AuthorizedUserInfo,
        /,
        *,
        pilot_db: PilotAgentsDB | None = None,
    ):

        if GENERIC_PILOT in pilot_info.properties:
            return

        if LIMITED_DELEGATION in pilot_info.properties:
            return

        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "you don't have the right properties"
        )
        return


RegisteredPilotAccessPolicyCallable = Annotated[
    Callable, Depends(RegisteredPilotAccessPolicy.check)
]
