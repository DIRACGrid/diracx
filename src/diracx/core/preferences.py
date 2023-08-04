from __future__ import annotations

__all__ = ("DiracxPreferences", "OutputFormats", "get_diracx_preferences")

import logging
from enum import Enum, StrEnum
from functools import lru_cache

from pydantic import AnyHttpUrl, BaseSettings, validator

from .utils import dotenv_files_from_environment


class OutputFormats(StrEnum):
    RICH = "RICH"
    JSON = "JSON"


class LogLevels(Enum):
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG


class DiracxPreferences(BaseSettings, env_prefix="DIRACX_"):
    url: AnyHttpUrl
    output_format: OutputFormats = OutputFormats.RICH
    log_level: LogLevels = LogLevels.INFO

    @classmethod
    def from_env(cls):
        return cls(_env_file=dotenv_files_from_environment("DIRACX_DOTENV"))

    @validator("log_level", pre=True)
    def validate_log_level(cls, v: str):
        if isinstance(v, str):
            return getattr(LogLevels, v.upper())
        return v


@lru_cache(maxsize=1)
def get_diracx_preferences() -> DiracxPreferences:
    """Caches the preferences."""
    return DiracxPreferences()
