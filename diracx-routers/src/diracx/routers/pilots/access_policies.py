from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.models import VectorSearchOperator, VectorSearchSpec
from diracx.core.properties import GENERIC_PILOT, SERVICE_ADMINISTRATOR
from diracx.db.sql.job.db import JobDB
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
        job_db: JobDB | None = None,
        job_ids: list[int] | None = None,
        allow_legacy_pilots: bool = False
    ):
        assert action, "action is a mandatory parameter"

        # Users can query
        # NOTE: Add into queries a VO constraint
        # To manage pilots, user have to be an admin
        # In some special cases (described with allow_legacy_pilots), we can allow pilots
        if action == ActionType.MANAGE_PILOTS:

            # To make it clear, we separate
            is_an_admin = SERVICE_ADMINISTRATOR in user_info.properties
            is_a_pilot_if_allowed = allow_legacy_pilots and GENERIC_PILOT in user_info.properties

            if not is_an_admin and not is_a_pilot_if_allowed:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have the permission to manage pilots.",
                )

        if action == ActionType.READ_PILOT_FIELDS:
            if GENERIC_PILOT in user_info.properties:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Pilots can't read other pilots info."
                )

        #
        # Additional checks if job_ids or pilot_stamps are provided
        #

        # First, if job_ids are provided, we check who is the owner
        if job_db and job_ids:
            job_owners = await job_db.job_summary(
                ["Owner", "VO"],
                [
                    VectorSearchSpec(
                        parameter="JobID",
                        operator=VectorSearchOperator.IN,
                        values=job_ids,
                    )
                ],
            )

            expected_owner = {
                "Owner": user_info.preferred_username,
                "VO": user_info.vo,
                "count": len(set(job_ids)),
            }
            # All the jobs belong to the user doing the query
            # and all of them are present
            if not job_owners == [expected_owner]:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have the rights to modify a pilot.",
                )

        # This is for example when we submit pilots, we use the user VO, so no need to verify
        if pilot_db and pilot_stamps:
            # Else, check its VO
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


CheckPilotManagementPolicyCallable = Annotated[
    Callable, Depends(PilotManagementAccessPolicy.check)
]
