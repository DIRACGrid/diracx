from __future__ import annotations

from typing import Annotated

from diracx.routers.auth import AuthorizedUserInfo, verify_dirac_access_token
from diracx.routers.fastapi_classes import DiracxRouter
from fastapi import Depends

from gubbins.db.sql import LollygagDB as _LollygagDB

# Define the dependency at the top, so you don't have to
# be so verbose in your routes
LollygagDB = Annotated[_LollygagDB, Depends(_LollygagDB.transaction)]

router = DiracxRouter()


@router.post("/insert_owner/{owner_name}")
async def insert_owner_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    lollygag_db: LollygagDB,
    owner_name: str,
):

    return await lollygag_db.insert_owner(owner_name)


@router.get("/get_owners")
async def get_owner_object(
    lollygag_db: LollygagDB,
):

    return await lollygag_db.get_owner()
