"""Minimal router for manual pilot submission and summary.

Demonstrates:
- Access policy definition
- Dependency injection for MyPilotDB
- Two endpoints for manual interaction
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.fastapi_classes import DiracxRouter
from diracx.routers.utils.users import AuthorizedUserInfo
from fastapi import Depends

from gubbins.db.sql import MyPilotDB as _MyPilotDB


class ActionType(StrEnum):
    CREATE = auto()
    READ = auto()


class MyPilotsAccessPolicy(BaseAccessPolicy):
    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
    ):
        assert action, "action is a mandatory parameter"


CheckMyPilotsPolicyCallable = Annotated[Callable, Depends(MyPilotsAccessPolicy.check)]

MyPilotDB = Annotated[_MyPilotDB, Depends(_MyPilotDB.transaction)]

router = DiracxRouter()


@router.post("/submit/{ce_name}")
async def submit_pilot(
    my_pilot_db: MyPilotDB,
    ce_name: str,
    check_permission: CheckMyPilotsPolicyCallable,
) -> dict:
    await check_permission(action=ActionType.CREATE)
    pilot_id = await my_pilot_db.submit_pilot(ce_name)
    return {"pilot_id": pilot_id}


@router.get("/summary")
async def get_pilot_summary(
    my_pilot_db: MyPilotDB,
    check_permission: CheckMyPilotsPolicyCallable,
) -> dict[str, int]:
    await check_permission(action=ActionType.READ)
    return await my_pilot_db.get_pilot_summary()
