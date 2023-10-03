"""
Illustrate how to extend/overwrite a diracx router.
It :
* changes slightly the return type
* uses the Gubbins specific configuration
* uses the Gubbins dependencies
"""

from diracx.routers.auth.well_known import Metadata
from diracx.routers.auth.well_known import (
    installation_metadata as _installation_metadata,
)
from diracx.routers.auth.well_known import router as diracx_wellknown_router
from diracx.routers.dependencies import DevelopmentSettings
from diracx.routers.fastapi_classes import DiracxRouter

from gubbins.routers.dependencies import Config

router = DiracxRouter(require_auth=False, path_root="")
router.include_router(diracx_wellknown_router)


# Change slightly the return type
class ExtendedMetadata(Metadata):
    gubbins_secrets: str
    gubbins_user_info: dict[str, list[str | None]]


# Overwrite the dirac-metadata endpoint and add an extra metadata
# This also makes sure  that we can get Config as a GubbinsConfig
@router.get("/dirac-metadata")
async def installation_metadata(
    config: Config,
    dev_settings: DevelopmentSettings,
) -> ExtendedMetadata:
    original_metadata = await _installation_metadata(config, dev_settings)

    gubbins_user_info: dict[str, list[str | None]] = {}
    for vo in config.Registry:
        vo_gubbins = [
            user.GubbinsSpecificInfo for user in config.Registry[vo].Users.values()
        ]
        gubbins_user_info[vo] = vo_gubbins

    gubbins_metadata = ExtendedMetadata(
        gubbins_secrets="hush!",
        virtual_organizations=original_metadata["virtual_organizations"],
        development_settings=original_metadata["development_settings"],
        gubbins_user_info=gubbins_user_info,
    )

    return gubbins_metadata
