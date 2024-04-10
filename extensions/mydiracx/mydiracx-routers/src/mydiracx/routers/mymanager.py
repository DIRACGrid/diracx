from __future__ import annotations

from typing import Annotated
from uuid import UUID

from diracx.routers.auth import AuthorizedUserInfo, verify_dirac_access_token
from diracx.routers.fastapi_classes import DiracxRouter
from fastapi import Depends

from mydiracx.db.sql import DummyDB as _DummyDB

# Database
DummyDB = Annotated[_DummyDB, Depends(_DummyDB.transaction)]

router = DiracxRouter()


@router.post("/insert_owner/{owner_name}")
async def insert_owner_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
    owner_name: str,
):

    return await dummy_db.insert_owner(owner_name)


@router.get("/get_owners")
async def get_owner_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):

    return await dummy_db.get_owner()


@router.post("/insert_car/{plate}/{model}/{owner_id}")
async def insert_car_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
    plate: UUID,
    model: str,
    owner_id: int,
):

    return await dummy_db.insert_car(plate, model, owner_id)


@router.get("/get_cars")
async def get_car_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):

    return await dummy_db.get_car()


@router.get("/get_summary")
async def get_summary(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):
    return await dummy_db.summary(["model"], {})
