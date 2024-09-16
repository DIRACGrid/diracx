from __future__ import annotations

__all__ = (
    "SqlalchemyDsn",
    "LocalFileUrl",
    "ServiceSettingsBase",
)

import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Annotated, Any, Self, TypeVar

from authlib.jose import JsonWebKey
from cryptography.fernet import Fernet
from pydantic import AnyUrl, BeforeValidator, SecretStr, TypeAdapter, UrlConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

T = TypeVar("T")

SqlalchemyDsn = Annotated[
    AnyUrl, UrlConstraints(allowed_schemes={"sqlite+aiosqlite", "mysql+aiomysql"})
]


class _TokenSigningKey(SecretStr):
    jwk: JsonWebKey

    def __init__(self, data: str):
        super().__init__(data)
        self.jwk = JsonWebKey.import_key(self.get_secret_value())


def _maybe_load_key_from_file(value: Any) -> Any:
    """Load private keys from files if needed."""
    if isinstance(value, str) and not value.strip().startswith("-----BEGIN"):
        url = TypeAdapter(LocalFileUrl).validate_python(value)
        if not url.scheme == "file":
            raise ValueError("Only file:// URLs are supported")
        if url.path is None:
            raise ValueError("No path specified")
        value = Path(url.path).read_text()
    return value


TokenSigningKey = Annotated[
    _TokenSigningKey, BeforeValidator(_maybe_load_key_from_file)
]


class FernetKey(SecretStr):
    fernet: Fernet

    def __init__(self, data: str):
        super().__init__(data)
        self.fernet = Fernet(self.get_secret_value())


def _apply_default_scheme(value: str) -> str:
    """Applies the default file:// scheme if not present."""
    if isinstance(value, str) and "://" not in value:
        value = f"file://{value}"
    return value


LocalFileUrl = Annotated[
    AnyUrl, UrlConstraints(host_required=False), BeforeValidator(_apply_default_scheme)
]


class ServiceSettingsBase(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    @classmethod
    def create(cls) -> Self:
        raise NotImplementedError("This should never be called")

    @contextlib.asynccontextmanager
    async def lifetime_function(self) -> AsyncIterator[None]:
        """A context manager that can be used to run code at startup and shutdown."""
        yield


class DevelopmentSettings(ServiceSettingsBase):
    """Settings for the Development Configuration that can influence run time."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_DEV_")

    # When then to true (only for demo/CI), crash if an access policy isn't
    # called
    crash_on_missed_access_policy: bool = False
