from __future__ import annotations

from typing import MutableMapping

from diracx.core.config import (
    Config as _Config,
)
from diracx.core.config import (
    RegistryConfig as _RegistryConfig,
)
from diracx.core.config import (
    UserConfig as _UserConfig,
)
from pydantic import Field

"""
In order to add extra config, you need to redefine
the whole tree down to the point you are interested in changing
"""


class UserConfig(_UserConfig):
    gubbins_specific_info: str | None = Field(None, alias="GubbinsSpecificInfo")


class RegistryConfig(_RegistryConfig):
    users: MutableMapping[str, UserConfig] = Field(alias="Users")  # type: ignore[assignment]


class Config(_Config):
    registry: MutableMapping[str, RegistryConfig] = Field(alias="Registry")  # type: ignore[assignment]
