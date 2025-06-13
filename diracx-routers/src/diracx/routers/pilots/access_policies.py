from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.exceptions import PilotNotFoundError
from diracx.core.properties import NORMAL_USER, TRUSTED_HOST
from diracx.db.sql import PilotAgentsDB
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    # Create a pilot
    CREATE_PILOT = auto()
    # Change some pilot fields
    CHANGE_PILOT_FIELD = auto()
    # Read some pilot info
    READ_PILOT_FIELDS = auto()


class PilotManagementAccessPolicy(BaseAccessPolicy):
    """Rules:
    * You need either NORMAL_USER in your properties
    * A NORMAL_USER can create a pilot.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        pilot_db: PilotAgentsDB | None = None,
        pilot_stamps: list[str] | None = None,
        vo: str | None = None,
        action: ActionType | None = None,
    ):
        assert action, "action is a mandatory parameter"

        if action == ActionType.READ_PILOT_FIELDS:
            if NORMAL_USER in user_info.properties:
                return

            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You have to be logged on to see pilots.",
            )

        if not vo:
            assert pilot_stamps and pilot_db, (
                "if vo is not provided, "
                "pilot_stamp and pilot_db are mandatory to determine the vo"
            )

            try:
                pilots = await pilot_db.get_pilots_by_stamp_bulk(pilot_stamps)
            except PilotNotFoundError as e:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="The given stamp is not associated with a pilot",
                ) from e

            # Semantic assured by get_pilots_by_stamp_bulk
            first_vo = pilots[0]["VO"]

            if not all(pilot["VO"] == first_vo for pilot in pilots):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You gave pilots with different VOs.",
                )

            vo = first_vo

        if not vo == user_info.vo:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have the right VO for this resource.",
            )

        if NORMAL_USER not in user_info.properties:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have the rights to create pilots.",
            )

        if action == ActionType.CREATE_PILOT:
            return

        if action == ActionType.CHANGE_PILOT_FIELD:
            return

        raise ValueError("Unknown action.")


CheckPilotManagementPolicyCallable = Annotated[
    Callable, Depends(PilotManagementAccessPolicy.check)
]


class DiracServicesAccessPolicy(BaseAccessPolicy):
    """This access policy is used by DIRAC services (ex: Matcher)."""

    @staticmethod
    async def policy(policy_name: str, user_info: AuthorizedUserInfo):
        if TRUSTED_HOST in user_info.properties:
            return

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint is reserved only for DIRAC services.",
        )


CheckDiracServicesPolicyCallable = Annotated[
    Callable, Depends(DiracServicesAccessPolicy.check)
]
