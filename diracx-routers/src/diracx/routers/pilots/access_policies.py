from __future__ import annotations

import logging
from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.exceptions import PilotNotFoundError
from diracx.core.models import ScalarSearchOperator, ScalarSearchSpec
from diracx.core.properties import NORMAL_USER, TRUSTED_HOST
from diracx.db.sql import PilotAgentsDB
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo

logger = logging.getLogger(__name__)


class ActionType(StrEnum):
    # Create a pilot or a secret
    CREATE_PILOT_OR_SECRET = auto()
    # Change some pilot fields
    CHANGE_PILOT_FIELD = auto()
    # Read some pilot info
    READ_PILOT_FIELDS = auto()
    #: Create/update pilot log records
    CREATE = auto()
    #: Search
    QUERY = auto()


class PilotManagementAccessPolicy(BaseAccessPolicy):
    """Rules:
    * You need either NORMAL_USER or TRUSTED_HOST in your properties
    * A NORMAL_USER can create a secret
    * A NORMAL_USER and TRUSTED_HOST can associate a pilot with a secret.
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
                detail="You don't have the rights to create secrets.",
            )

        if action == ActionType.CREATE_PILOT_OR_SECRET:
            return

        if action == ActionType.CHANGE_PILOT_FIELD:
            # TODO: Tailor, user-specific?
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


class PilotLogsAccessPolicy(BaseAccessPolicy):
    """Rules:
    Only NORMAL_USER in a correct VO and a diracAdmin VO member can query log records.
    All other actions and users are explicitly denied access.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
        pilot_agents_db: PilotAgentsDB | None = None,
        pilot_id: int | None = None,
    ):
        assert pilot_agents_db
        if action is None:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, detail="Action is a mandatory argument"
            )
        elif action == ActionType.QUERY:
            if pilot_id is None:
                logger.error("Pilot ID value is not provided (None)")
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    detail=f"PilotID not provided: {pilot_id}",
                )
            search_params = ScalarSearchSpec(
                parameter="PilotID",
                operator=ScalarSearchOperator.EQUAL,
                value=pilot_id,
            )

            total, result = await pilot_agents_db.search(["VO"], [search_params], [])
            # we expect exactly one row.
            if total != 1:
                logger.error(
                    "Cannot determine VO for requested PilotID: %d, found %d candidates.",
                    pilot_id,
                    total,
                )
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST, detail=f"PilotID not found: {pilot_id}"
                )
            vo = result[0]["VO"]

            if user_info.vo == "diracAdmin":
                return

            if NORMAL_USER in user_info.properties and user_info.vo == vo:
                return

            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to access this pilot's log.",
            )
        else:
            raise NotImplementedError(action)


CheckPilotLogsPolicyCallable = Annotated[Callable, Depends(PilotLogsAccessPolicy.check)]
