from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

__all__ = ("DiracxPreferences", "OutputFormats", "get_diracx_preferences")

import logging
import sys
from enum import Enum, StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings

from .utils import dotenv_files_from_environment


class OutputFormats(StrEnum):
    RICH = "RICH"
    JSON = "JSON"

    @classmethod
    def default(cls):
        return cls.RICH if sys.stdout.isatty() else cls.JSON


class LogLevels(Enum):
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG


class DiracxPreferences(BaseSettings):
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
        return cls(_env_file=dotenv_files_from_environment("DIRACX_DOTENV"))

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str):
        if isinstance(v, str):
            return getattr(LogLevels, v.upper())
        return v


@lru_cache(maxsize=1)
def get_diracx_preferences() -> DiracxPreferences:
    """Caches the preferences."""
    return DiracxPreferences()
