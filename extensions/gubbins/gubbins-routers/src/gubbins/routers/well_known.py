"""
Illustrate how to extend/overwrite a diracx router.
It :
* changes slightly the return type
* uses the Gubbins specific configuration
* uses the Gubbins dependencies
"""

from __future__ import annotations

__all__ = ["get_installation_metadata"]

from diracx.routers.auth import router as diracx_wellknown_router
from diracx.routers.fastapi_classes import DiracxRouter

from gubbins.core.models import ExtendedMetadata
from gubbins.logic.auth import (
    get_installation_metadata as get_installation_metadata_bl,
)
from gubbins.routers.dependencies import Config

router = DiracxRouter(require_auth=False, path_root="")
router.include_router(diracx_wellknown_router)


# Overwrite the dirac-metadata endpoint and add an extra metadata
# This also makes sure  that we can get Config as a GubbinsConfig
@router.get("/dirac-metadata")
async def get_installation_metadata(
    config: Config,
) -> ExtendedMetadata:
    return await get_installation_metadata_bl(config)
