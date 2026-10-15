"""Expose health probes for Kubernetes and other orchestration systems.

The liveness probe checks that configuration is available, while readiness
and startup probes also verify connectivity to the authentication database.
"""

from __future__ import annotations

import logging

from fastapi import HTTPException
from starlette.responses import JSONResponse

from diracx.db.sql import AuthDB
from diracx.routers.dependencies import Config

from ..fastapi_classes import DiracxRouter

logger = logging.getLogger(__name__)

router = DiracxRouter(require_auth=False)


@router.get("/live", include_in_schema=False)
async def liveness(config: Config):
    """Report whether the application is alive.

    The configuration dependency ensures this probe fails if the configuration
    expires and cannot be refreshed.

    Args:
        config: Loaded DiracX configuration required for the probe to succeed.

    Returns:
        A JSON response with the ``live`` status.
    """
    assert (
        config is not None
    )  # Depend on the config so we know it's loaded successfully
    return JSONResponse(content={"status": "live"})


@router.get("/ready", include_in_schema=False)
async def ready(config: Config, auth_db: AuthDB):
    """Report whether the application is ready to serve requests.

    The probe requires loaded configuration and a successful authentication
    database ping.

    Args:
        config: Loaded DiracX configuration required for the probe to succeed.
        auth_db: Authentication database to check for connectivity.

    Returns:
        A JSON response with the ``ready`` status.

    Raises:
        HTTPException: If the authentication database ping fails (503).
    """
    assert (
        config is not None
    )  # Depend on the config so we know it's loaded successfully
    try:
        await auth_db.ping()
    except Exception as e:
        logger.warning("Ready probe failed: AuthDB ping failed", exc_info=True)
        raise HTTPException(status_code=503, detail="AuthDB ping failed") from e
    return JSONResponse(content={"status": "ready"})


@router.get("/startup", include_in_schema=False)
async def startup(config: Config, auth_db: AuthDB):
    """Report whether startup dependencies are available.

    Startup completes when configuration is loaded and the authentication
    database responds to a ping.

    Args:
        config: Loaded DiracX configuration required for the probe to succeed.
        auth_db: Authentication database to check for connectivity.

    Returns:
        A JSON response with the ``startup complete`` status.

    Raises:
        HTTPException: If the authentication database ping fails (503).
    """
    assert (
        config is not None
    )  # Depend on the config so we know it's loaded successfully
    try:
        await auth_db.ping()
    except Exception as e:
        logger.warning("Startup probe failed: AuthDB ping failed", exc_info=True)
        raise HTTPException(status_code=503, detail="AuthDB ping failed") from e
    return JSONResponse(content={"status": "startup complete"})
