"""Configuration module: Provides tools for managing backend configurations."""

from __future__ import annotations

__all__ = [
    # Schema
    "Config",
    "DIRACConfig",
    "GroupConfig",
    "IdpConfig",
    "OperationsConfig",
    "RegistryConfig",
    "SerializableSet",
    "SupportInfo",
    "UserConfig",
    # Sources
    "ConfigSource",
    "ConfigSourceUrl",
    "LocalGitConfigSource",
    "RemoteGitConfigSource",
    "is_running_in_async_context",
]

from .schema import (
    Config,
    DIRACConfig,
    GroupConfig,
    IdpConfig,
    OperationsConfig,
    RegistryConfig,
    SerializableSet,
    SupportInfo,
    UserConfig,
)
from .sources import (
    ConfigSource,
    ConfigSourceUrl,
    LocalGitConfigSource,
    RemoteGitConfigSource,
    is_running_in_async_context,
)
