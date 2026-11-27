"""Gubbins extension of the DiracX configuration schema.

In order to add extra config, you need to redefine
the whole tree down to the point you are interested in changing.
"""

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


class UserConfig(_UserConfig):
    """User configuration extended with Gubbins-specific information."""

    gubbins_specific_info: str | None = Field(None, alias="GubbinsSpecificInfo")
    """Extra, Gubbins-specific information about the user."""


class RegistryConfig(_RegistryConfig):
    """Registry configuration using the extended user configuration."""

    users: MutableMapping[str, UserConfig] = Field(alias="Users")  # type: ignore[assignment]
    """Users of the VO, with their Gubbins-specific information."""


class Config(_Config):
    """Top-level configuration using the extended registry configuration."""

    registry: MutableMapping[str, RegistryConfig] = Field(alias="Registry")  # type: ignore[assignment]
    """Registry of each VO, using the extended configuration."""
