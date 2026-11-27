"""This router makes use of the new LollygagDB.

It uses the Lollygag AccessPolicy (which itself requires the Gubbins property).
"""

from __future__ import annotations

from diracx.routers.fastapi_classes import DiracxRouter

from gubbins.db.sql import LollygagDB
from gubbins.logic.lollygag import (
    get_gubbins_secrets as get_gubbins_secrets_bl,
)
from gubbins.logic.lollygag import get_owner_object as get_owner_object_bl
from gubbins.logic.lollygag import (
    insert_owner_object as insert_owner_object_bl,
)

from .access_policy import ActionType, CheckLollygagPolicyCallable

router = DiracxRouter()


@router.post("/insert_owner/{owner_name}")
async def insert_owner_object(
    lollygag_db: LollygagDB,
    owner_name: str,
    check_permission: CheckLollygagPolicyCallable,
):
    """Insert an owner.

    \f

    Args:
        lollygag_db: Lollygag database.
        owner_name: Name of the owner.
        check_permission: Checks that the user may create objects.

    Returns:
        The ID of the new owner.
    """
    await check_permission(action=ActionType.CREATE)
    return await insert_owner_object_bl(lollygag_db, owner_name)


@router.get("/get_owners")
async def get_owner_object(
    lollygag_db: LollygagDB,
    check_permission: CheckLollygagPolicyCallable,
):
    """Get the names of all the owners.

    \f

    Args:
        lollygag_db: Lollygag database.
        check_permission: Checks that the user may read objects.

    Returns:
        The owner names.
    """
    await check_permission(action=ActionType.READ)
    return await get_owner_object_bl(lollygag_db)


@router.get("/gubbins_sensei")
async def get_gubbins_secrets(
    lollygag_db: LollygagDB,
    check_permission: CheckLollygagPolicyCallable,
):
    """Does nothing but expects a GUBBINS_SENSEI permission.

    \f

    Args:
        lollygag_db: Lollygag database.
        check_permission: Checks that the user may manage objects, which
            requires the ``GUBBINS_SENSEI`` property.

    Returns:
        The owner names.
    """
    await check_permission(action=ActionType.MANAGE)
    return await get_gubbins_secrets_bl(lollygag_db)
