"""
This router makes use of the new LollygagDB.
It uses the the Lollygag AccessPolicy (which itself requires the Gubbins property)
"""

from __future__ import annotations

from typing import Annotated

from diracx.routers.fastapi_classes import DiracxRouter
from fastapi import Depends

from gubbins.db.sql import LollygagDB as _LollygagDB
from gubbins.logic.lollygag.lollygag import (
    get_gubbins_secrets as get_gubbins_secrets_bl,
)
from gubbins.logic.lollygag.lollygag import get_owner_object as get_owner_object_bl
from gubbins.logic.lollygag.lollygag import (
    insert_owner_object as insert_owner_object_bl,
)

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
    return await insert_owner_object_bl(lollygag_db, owner_name)


@router.get("/get_owners")
async def get_owner_object(
    lollygag_db: LollygagDB,
    check_permission: CheckLollygagPolicyCallable,
):
    await check_permission(action=ActionType.READ)
    return await get_owner_object_bl(lollygag_db)


@router.get("/gubbins_sensei")
async def get_gubbins_secrets(
    lollygag_db: LollygagDB,
    check_permission: CheckLollygagPolicyCallable,
):
    """Does nothing but expects a GUBBINS_SENSEI permission"""
    await check_permission(action=ActionType.MANAGE)
    return await get_gubbins_secrets_bl(lollygag_db)
