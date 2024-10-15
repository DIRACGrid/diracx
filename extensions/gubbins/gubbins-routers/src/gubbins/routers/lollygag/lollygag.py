"""
This router makes use of the new LollygagDB.
It uses the the Lollygag AccessPolicy (which itself requires the Gubbins property)
"""

from __future__ import annotations

from typing import Annotated

from diracx.routers.fastapi_classes import DiracxRouter
from fastapi import Depends

from gubbins.db.sql import LollygagDB as _LollygagDB

from .access_policy import ActionType, CheckLollygagPolicyCallable

# Define the dependency at the top, so you don't have to
# be so verbose in your routes
LollygagDB = Annotated[_LollygagDB, Depends(_LollygagDB.transaction)]

router = DiracxRouter()


@router.post("/insert_owner/{owner_name}")
async def insert_owner_object(
    lollygag_db: LollygagDB,
    owner_name: str,
    check_permission: CheckLollygagPolicyCallable,
):
    await check_permission(action=ActionType.CREATE)
    return await lollygag_db.insert_owner(owner_name)


@router.get("/get_owners")
async def get_owner_object(
    lollygag_db: LollygagDB,
    check_permission: CheckLollygagPolicyCallable,
):
    await check_permission(action=ActionType.READ)
    return await lollygag_db.get_owner()


@router.get("/gubbins_sensei")
async def get_gubbins_secrets(
    lollygag_db: LollygagDB,
    check_permission: CheckLollygagPolicyCallable,
):
    """Does nothing but expects a GUBBINS_SENSEI permission"""
    await check_permission(action=ActionType.MANAGE)
    return await lollygag_db.get_owner()
