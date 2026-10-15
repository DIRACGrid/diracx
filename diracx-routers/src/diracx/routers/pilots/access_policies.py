"""Define access rules for pilot management and pilot metadata reads.

The policy scopes regular users to their own VO, permits service
administrators cross-VO access, and optionally permits legacy pilots to
manage a limited set of pilot records.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from http import HTTPStatus
from typing import Annotated

from fastapi import Depends, HTTPException

from diracx.core.properties import GENERIC_PILOT, SERVICE_ADMINISTRATOR
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.logic.pilots.query import get_pilots_by_stamp
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    """Operations protected by the pilot access policy."""

    # Change pilot metadata (status, fields, etc.). Admin-only by default;
    # legacy pilot X.509 identities can be allowed via `allow_legacy_pilots`.
    MANAGE_PILOTS = auto()
    # Read pilot metadata. Normal users can read their own VO's pilots;
    # `SERVICE_ADMINISTRATOR` can read across VOs.
    READ_PILOT_METADATA = auto()


class PilotManagementAccessPolicy(BaseAccessPolicy):
    """Authorize pilot management and metadata reads.

    Users can read pilot metadata in their own VO. Service administrators can
    read across VOs and manage pilots. When enabled by a route, legacy X.509
    pilot identities can manage one pilot per call, but cannot read pilot
    metadata.
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
        target_vo: str | None = None,
        allow_legacy_pilots: bool = False,
    ):
        """Check whether the caller may perform the requested pilot operation.

        Authorization is scoped to the caller's VO, not to ownership of a
        specific pilot stamp. Service administrators are exempt from VO
        restrictions.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated identity and security properties.
            action: Pilot operation to authorize.
            pilot_db: Database used to verify pilot stamps and their VOs.
            pilot_stamps: Pilot stamps affected by the operation.
            target_vo: VO for which new pilots are being registered.
            allow_legacy_pilots: Whether generic pilot identities may manage pilots.

        Raises:
            ValueError: If the required action is not provided.
            HTTPException: If the caller lacks permission (403), a requested
                pilot does not exist (404), or a requested pilot is outside
                the caller's VO (403).
        """
        # Authorization is VO-scoped, not bound to the caller's
        # own pilot stamp. This mirrors DIRAC's PilotManagerHandler, which has
        # no ownership check either.
        if action is None:
            raise ValueError("action is a mandatory parameter")

        # Service administrators read and manage pilots across VOs; every
        # other identity is confined to its own VO.
        is_admin = SERVICE_ADMINISTRATOR in user_info.properties

        if action == ActionType.MANAGE_PILOTS:
            is_legacy_pilot = (
                allow_legacy_pilots and GENERIC_PILOT in user_info.properties
            )
            if not is_admin and not is_legacy_pilot:
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail="Insufficient permissions to manage pilots.",
                )
            # Limit the damage a stolen legacy pilot credential can do: a
            # pilot identity may only act on a single pilot per call. Note
            # this bounds the rate of abuse, not its scope.
            if not is_admin and pilot_stamps and len(set(pilot_stamps)) > 1:
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail="Insufficient permissions to modify more than one pilot.",
                )
            # `target_vo` is the VO that pilots are being registered into
            # (pilots that already exist are checked against the DB through
            # `pilot_stamps` below). Only service administrators may
            # register pilots outside their own VO.
            if target_vo is not None and not is_admin and target_vo != user_info.vo:
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail="Pilots can only be registered for your own VO.",
                )

        if action == ActionType.READ_PILOT_METADATA:
            if GENERIC_PILOT in user_info.properties:
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail="Pilots cannot read other pilots' metadata.",
                )

        # If pilot stamps are provided, verify they all exist and (unless the
        # caller is a service administrator) belong to the user's VO.
        if pilot_db is not None and pilot_stamps:
            pilots = await get_pilots_by_stamp(
                pilot_db=pilot_db,
                pilot_stamps=pilot_stamps,
                parameters=["VO"],
            )
            if len(pilots) != len(set(pilot_stamps)):
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail="At least one pilot does not exist.",
                )
            if not is_admin and not all(
                pilot["VO"] == user_info.vo for pilot in pilots
            ):
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail=(
                        "Insufficient permissions to access all of the provided pilots."
                    ),
                )


CheckPilotManagementPolicyCallable = Annotated[
    Callable, Depends(PilotManagementAccessPolicy.check)
]
