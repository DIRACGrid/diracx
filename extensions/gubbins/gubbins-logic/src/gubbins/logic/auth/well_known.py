from diracx.logic.auth.well_known import (
    get_installation_metadata as get_general_installation_metadata,
)

from gubbins.core.config.schema import Config
from gubbins.core.models import ExtendedMetadata


async def get_installation_metadata(
    config: Config,
) -> ExtendedMetadata:
    """Get metadata about the dirac installation."""
    original_metadata = await get_general_installation_metadata(config)

    gubbins_user_info: dict[str, list[str | None]] = {}
    for vo in config.Registry:
        vo_gubbins = [
            user.GubbinsSpecificInfo for user in config.Registry[vo].Users.values()
        ]
        gubbins_user_info[vo] = vo_gubbins

    gubbins_metadata = ExtendedMetadata(
        gubbins_secrets="hush!",
        virtual_organizations=original_metadata["virtual_organizations"],
        gubbins_user_info=gubbins_user_info,
    )

    return gubbins_metadata
