from diracx.routers.auth import router as diracx_auth_router
from diracx.routers.fastapi_classes import DiracxRouter
from fastapi import HTTPException, status

router = DiracxRouter(require_auth=False)
router.include_router(diracx_auth_router)

# Just overriding does not work: https://github.com/tiangolo/fastapi/discussions/8489


@router.post("/device")
async def forbid_device_flow(client_id: str, scope: str, *args, **kwargs):
    """Initiate the device flow against DIRAC authorization Server.
    Scope must have exactly up to one `group` (otherwise default) and
    one or more `property` scope.
    If no property, then get default one.

    Offers the user to go with the browser to
    `auth/<vo>/device?user_code=XYZ`
    """
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="BOOOOOM")
