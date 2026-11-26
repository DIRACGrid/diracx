"""Minimal router for manual pilot submission and summary.

Demonstrates:
- Access policy definition
- Dependency injection for MyPilotDB
- Two endpoints for manual interaction
"""

from __future__ import annotations

__all__ = ["MyPilotsAccessPolicy", "router"]

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.fastapi_classes import DiracxRouter
from diracx.routers.utils.users import AuthorizedUserInfo
from fastapi import Depends

from gubbins.db.sql import MyPilotDB


class ActionType(StrEnum):
    """Actions checked by the pilot policy."""

    # Submit a pilot
    CREATE = auto()
    # Read the pilot summary
    READ = auto()


class MyPilotsAccessPolicy(BaseAccessPolicy):
    """Authorize the pilot actions.

    Every authenticated user may perform them.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
    ):
        """Check that an action is given.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated user identity and security properties.
            action: Pilot action to authorize.
        """
        assert action, "action is a mandatory parameter"


CheckMyPilotsPolicyCallable = Annotated[Callable, Depends(MyPilotsAccessPolicy.check)]

router = DiracxRouter()


@router.post("/submit/{ce_name}")
async def submit_pilot(
    my_pilot_db: MyPilotDB,
    ce_name: str,
    check_permission: CheckMyPilotsPolicyCallable,
) -> dict:
    """Submit a pilot to a compute element.

    \f

    Args:
        my_pilot_db: Pilot database.
        ce_name: Name of the compute element.
        check_permission: Checks that the user may create pilots.

    Returns:
        The ID of the new pilot submission, as ``{"pilot_id": ...}``.
    """
    await check_permission(action=ActionType.CREATE)
    pilot_id = await my_pilot_db.submit_pilot(ce_name)
    return {"pilot_id": pilot_id}


@router.get("/summary")
async def get_pilot_summary(
    my_pilot_db: MyPilotDB,
    check_permission: CheckMyPilotsPolicyCallable,
) -> dict[str, int]:
    """Get the number of pilots by status.

    \f

    Args:
        my_pilot_db: Pilot database.
        check_permission: Checks that the user may read the summary.

    Returns:
        The number of pilots for each status.
    """
    await check_permission(action=ActionType.READ)
    return await my_pilot_db.get_pilot_summary()
