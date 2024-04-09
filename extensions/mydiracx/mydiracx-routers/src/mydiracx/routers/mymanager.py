from __future__ import annotations

from uuid import UUID

from diracx.routers.fastapi_classes import DiracxRouter
from fastapi.security import OpenIdConnect

from .dependencies import (
    DummyDB,
)

oidc_scheme = OpenIdConnect(openIdConnectUrl="/.well-known/openid-configuration")


router = DiracxRouter(require_auth=False)

@router.post("/insert_owner/{owner_name}")
async def insert_example_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
    owner_name: str,
):

    return await dummy_db.insert_owner(owner_name)

@router.get("/get_owners")
async def insert_example_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):

    return await dummy_db.get_owner()

@router.post("/insert_car/{plate}/{model}/{owner_id}")
async def get_example_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
    plate: UUID,
    model: str,
    owner_id: int,
):

    return await dummy_db.insert_car(plate, model, owner_id)

@router.get("/get_cars")
async def insert_example_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):

    return await dummy_db.get_car()

@router.get("/get_summary")
async def get_example_object(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):
    return await dummy_db.summary(['model'],{})