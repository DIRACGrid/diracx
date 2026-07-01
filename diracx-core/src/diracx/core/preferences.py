"""Application preferences and configuration handling.

This module defines the DIRACX settings model and helpers for loading
preferences from environment variables and dotenv files.
"""

from __future__ import annotations

__all__ = [
    "DiracxPreferences",
    "OutputFormats",
    "get_diracx_preferences",
]

import logging
import sys
from enum import Enum, StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import AnyHttpUrl, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .utils import dotenv_files_from_environment


class OutputFormats(StrEnum):
    """Supported output formats for CLI rendering."""

    RICH = "RICH"
    JSON = "JSON"

    @classmethod
    def default(cls):
        """Return the default output format for the current terminal.

        Returns:
            OutputFormats: Rich output for interactive terminals and JSON otherwise.
        """
        return cls.RICH if sys.stdout.isatty() else cls.JSON


class LogLevels(Enum):
    """Supported logging levels for DIRACX settings."""

    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG


class DiracxPreferences(BaseSettings):
    """Runtime preferences loaded from environment variables and dotenv files."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_")

    url: AnyHttpUrl
    ca_path: Path | None = None
    output_format: OutputFormats = Field(default_factory=OutputFormats.default)
    log_level: LogLevels = LogLevels.INFO
    credentials_path: Path = Field(
        default_factory=lambda: Path.home() / ".cache" / "diracx" / "credentials.json"
    )

    @classmethod
    def from_env(cls):
        """Create preferences from environment variables and dotenv files.

        Returns:
            DiracxPreferences: Preferences loaded from the detected dotenv files.
        """
        return cls(_env_file=dotenv_files_from_environment("DIRACX_DOTENV"))

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str):
        """Validate and normalize a logging-level input value.

        Args:
            v (str): Logging level provided by the environment.

        Returns:
            LogLevels | Any: The normalized logging level value.
        """
        if isinstance(v, str):
            return getattr(LogLevels, v.upper())
        return v


@lru_cache(maxsize=1)
def get_diracx_preferences() -> DiracxPreferences:
    """Return cached DIRACX preferences for the current process.

    Returns:
        DiracxPreferences: The lazily initialized preferences instance.
    """
    return DiracxPreferences()
