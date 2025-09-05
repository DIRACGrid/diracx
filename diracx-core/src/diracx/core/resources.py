from __future__ import annotations

__all__ = ["find_compatible_platforms"]


from DIRACCommon.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform
from DIRACCommon.Core.Utilities.ReturnValues import returnValueOrRaise

from diracx.core.config import Config
from diracx.core.extensions import supports_extending


@supports_extending("diracx.resources", "find_compatible_platforms")
def find_compatible_platforms(job_platforms: list[str], config: Config) -> list[str]:
    """Find compatible platforms for the given job platforms.

    This is the default implementation, it can be overridden by the user to
    provide a custom implementation using the "find_compatible_platforms"
    key in "diracx" entrypoint.

    Args:
        job_platforms: list of job platforms
        config: config object

    Returns:
        list of compatible platforms

    """
    os_compatibility_dict = config.Resources.Computing.OSCompatibility
    return returnValueOrRaise(getDIRACPlatform(job_platforms, os_compatibility_dict))
