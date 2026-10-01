"""User preferences and environment-backed settings for DiracX."""

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
    """Output formats supported by the DiracX CLI."""

    RICH = "RICH"
    JSON = "JSON"

    @classmethod
    def default(cls):
        """Select the default output format for the current terminal.

        Returns:
            Rich output for interactive terminals, otherwise JSON output.
        """
        return cls.RICH if sys.stdout.isatty() else cls.JSON


class LogLevels(Enum):
    """Logging levels supported by DiracX preferences."""

    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG


class DiracxPreferences(BaseSettings):
    """Environment-backed preferences used by DiracX clients.

    Attributes:
        url: Base URL of the DiracX service.
        ca_path: Optional path to the certificate authority bundle.
        output_format: Format used to render command output.
        log_level: Logging level for the client.
        credentials_path: Path to the cached credentials file.
    """

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
        """Create preferences using dotenv files selected by the environment.

        Returns:
            Preferences loaded from the environment and configured dotenv files.
        """
        return cls(_env_file=dotenv_files_from_environment("DIRACX_DOTENV"))

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str):
        """Convert a string log level to its enum value.

        Args:
            v: Log level value to validate.

        Returns:
            The matching log level enum, or the original value if already parsed.
        """
        if isinstance(v, str):
            return getattr(LogLevels, v.upper())
        return v


@lru_cache(maxsize=1)
def get_diracx_preferences() -> DiracxPreferences:
    """Return the cached DiracX preferences.

    Returns:
        The process-wide DiracX preferences instance.
    """
    return DiracxPreferences()
