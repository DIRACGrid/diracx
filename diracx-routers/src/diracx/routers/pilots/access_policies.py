from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import SERVICE_ADMINISTRATOR
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.logic.pilots.query import get_pilots_by_stamp
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    # Change some pilot fields
    MANAGE_PILOTS = auto()
    # Read some pilot info
    READ_PILOT_FIELDS = auto()


class PilotManagementAccessPolicy(BaseAccessPolicy):
    """Rules:
    * Every user can access data about his VO
    * An administrator can modify a pilot.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
        pilot_db: PilotAgentsDB | None = None,
        pilot_stamps: list[str] | None = None,
    ):
        assert action, "action is a mandatory parameter"

        # Users can query
        # NOTE: Add into queries a VO constraint
        if action == ActionType.READ_PILOT_FIELDS:
            return

        # If we want to modify pilots, we allow only admins
        # TODO: See if we add other types of admins
        if SERVICE_ADMINISTRATOR in user_info.properties:
            # If we don't provide pilot_db and pilot_stamps, we accept directly
            # This is for example when we submit pilots, we use the user VO, so no need to verify
            if not (pilot_db and pilot_stamps):
                return

            # Else, check its VO
            assert pilot_db, "PilotDB is needed to determine pilot VO."
            assert pilot_stamps, "PilotStamps are needed to determine pilot VO."

            pilots = await get_pilots_by_stamp(
                pilot_db=pilot_db,
                pilot_stamps=pilot_stamps,
                parameters=["VO"],
                allow_missing=True,
            )

            if len(pilots) != len(pilot_stamps):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="At least one pilot does not exist.",
                )

            if not all(pilot["VO"] == user_info.vo for pilot in pilots):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have access to all pilots.",
                )

            return

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have the rights to modify a pilot.",
        )


CheckPilotManagementPolicyCallable = Annotated[
    Callable, Depends(PilotManagementAccessPolicy.check)
]
