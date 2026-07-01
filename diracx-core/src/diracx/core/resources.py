from __future__ import annotations

__all__ = ["find_compatible_platforms"]


from DIRACCommon.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform
from DIRACCommon.Core.Utilities.ReturnValues import returnValueOrRaise

from .config import Config
from .extensions import DiracEntryPoint, supports_extending


@supports_extending(DiracEntryPoint.RESOURCES, "find_compatible_platforms")
def find_compatible_platforms(job_platforms: list[str], config: Config) -> list[str]:
    """Return compatible DIRAC platforms for the supplied job platforms.

    This default implementation can be overridden by providing a custom
    implementation under the ``find_compatible_platforms`` key in the
    ``diracx`` entrypoint.

    Args:
        job_platforms (list[str]): Job platform identifiers to evaluate.
        config (Config): Application configuration.

    Returns:
        list[str]: Compatible platforms for the requested job platforms.
    """
    os_compatibility_dict = config.resources.computing.os_compatibility
    return returnValueOrRaise(getDIRACPlatform(job_platforms, os_compatibility_dict))
