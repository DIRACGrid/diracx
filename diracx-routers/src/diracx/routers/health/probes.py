"""Health probes for use in Kubernetes and other orchestration systems."""

from __future__ import annotations

__all__ = ["router"]

from fastapi import HTTPException
from starlette.responses import JSONResponse

from ..dependencies import AuthDB, Config
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False)


@router.get("/live", include_in_schema=False)
async def liveness():
    """Returns a simple status to indicate the app is running."""
    return JSONResponse(content={"status": "live"})


@router.get("/ready", include_in_schema=False)
async def readiness(config: Config, auth_db: AuthDB):
    """Readiness endpoint.

    Checks if at least the configuration is loaded and the AuthDB database
    connection is available.
    """
    if not any(vo_registry.Users for vo_registry in config.Registry.values()):
        raise HTTPException(status_code=503, detail="No users in registry")
    try:
        await auth_db.ping()
    except Exception as e:
        raise HTTPException(status_code=503, detail="AuthDB ping failed") from e
    return JSONResponse(content={"status": "ready"})


@router.get("/startup", include_in_schema=False)
async def startup_check(config: Config, auth_db: AuthDB):
    """Startup endpoint.

    Checks if at least the configuration is loaded and the AuthDB database
    connection is available.
    """
    if not any(vo_registry.Users for vo_registry in config.Registry.values()):
        raise HTTPException(status_code=503, detail="No users in registry")
    try:
        await auth_db.ping()
    except Exception as e:
        raise HTTPException(status_code=503, detail="AuthDB ping failed") from e
    return JSONResponse(content={"status": "startup complete"})
