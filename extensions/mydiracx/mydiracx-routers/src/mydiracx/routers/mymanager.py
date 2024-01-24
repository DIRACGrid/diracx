from __future__ import annotations

from diracx.routers.fastapi_classes import DiracxRouter
from fastapi.security import OpenIdConnect

from .dependencies import (
    DummyDB,
)

oidc_scheme = OpenIdConnect(openIdConnectUrl="/.well-known/openid-configuration")


router = DiracxRouter(require_auth=False)


@router.get("/example/get_summary")
async def get_example_object(
    # user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):
    # return await dummy_db.summary()
    return
