from __future__ import annotations

from typing import MutableMapping

from diracx.core.config.schema import (
    Config as _Config,
)
from diracx.core.config.schema import (
    RegistryConfig as _RegistryConfig,
)
from diracx.core.config.schema import (
    UserConfig as _UserConfig,
)

"""
In order to add extra config, you need to redefine
the whole tree down to the point you are interested in changing
"""


class UserConfig(_UserConfig):
    GubbinsSpecificInfo: str | None = None


class RegistryConfig(_RegistryConfig):
    Users: MutableMapping[str, UserConfig]  # type: ignore[assignment]


class Config(_Config):
    Registry: MutableMapping[str, RegistryConfig]  # type: ignore[assignment]
