from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.models.search import VectorSearchOperator, VectorSearchSpec
from diracx.core.properties import GENERIC_PILOT, SERVICE_ADMINISTRATOR
from diracx.db.sql.job.db import JobDB
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.logic.pilots.query import get_pilots_by_stamp
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    # Change pilot metadata (status, fields, etc.). Admin-only by default;
    # legacy pilot X.509 identities can be allowed via `allow_legacy_pilots`.
    MANAGE_PILOTS = auto()
    # Read pilot metadata. Normal users can read their own VO's pilots;
    # `SERVICE_ADMINISTRATOR` can read across VOs.
    READ_PILOT_METADATA = auto()


class PilotManagementAccessPolicy(BaseAccessPolicy):
    """Pilot management access policy.

    * Every user can read pilots from their own VO.
    * Service administrators can read across VOs and manage pilots.
    * Legacy X.509 pilot identities may be allowed to manage themselves when
      `allow_legacy_pilots=True` is passed by the route.
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
        allow_legacy_pilots: bool = False,
    ):
        # Authorization is VO-scoped, not bound to the caller's
        # own pilot stamp. This mirrors DIRAC's PilotManagerHandler, which has
        # no ownership check either.
        if action is None:
            raise ValueError("action is a mandatory parameter")

        if action == ActionType.MANAGE_PILOTS:
            is_admin = SERVICE_ADMINISTRATOR in user_info.properties
            is_legacy_pilot = (
                allow_legacy_pilots and GENERIC_PILOT in user_info.properties
            )
            if not is_admin and not is_legacy_pilot:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Insufficient permissions to manage pilots.",
                )

        if action == ActionType.READ_PILOT_METADATA:
            if GENERIC_PILOT in user_info.properties:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Pilots cannot read other pilots' metadata.",
                )

        # If job IDs are provided, verify the user owns all of them.
        # Using a direct search (rather than summary/aggregate equality) is
        # clearer and gives a distinct 404 vs 403 on missing jobs.
        if job_db is not None and job_ids:
            _, owner_rows = await job_db.search(
                parameters=["Owner", "VO"],
                search=[
                    VectorSearchSpec(
                        parameter="JobID",
                        operator=VectorSearchOperator.IN,
                        values=job_ids,
                    )
                ],
                sorts=[],
                per_page=len(set(job_ids)),
            )
            if len(owner_rows) != len(set(job_ids)):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="One or more jobs do not exist.",
                )
            if not all(
                row["Owner"] == user_info.preferred_username
                and row["VO"] == user_info.vo
                for row in owner_rows
            ):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        "Insufficient permissions to access all of the provided jobs."
                    ),
                )

        # If pilot stamps are provided, verify they all belong to the user's VO.
        if pilot_db is not None and pilot_stamps:
            pilots = await get_pilots_by_stamp(
                pilot_db=pilot_db,
                pilot_stamps=pilot_stamps,
                parameters=["VO"],
            )
            if len(pilots) != len(set(pilot_stamps)):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="At least one pilot does not exist.",
                )
            if not all(pilot["VO"] == user_info.vo for pilot in pilots):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        "Insufficient permissions to access all of the provided pilots."
                    ),
                )


CheckPilotManagementPolicyCallable = Annotated[
    Callable, Depends(PilotManagementAccessPolicy.check)
]
