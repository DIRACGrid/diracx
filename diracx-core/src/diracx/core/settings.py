from __future__ import annotations

from diracx.core.properties import SecurityProperty

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
from pydantic import (
    AnyUrl,
    BeforeValidator,
    Field,
    FileUrl,
    SecretStr,
    TypeAdapter,
    UrlConstraints,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

T = TypeVar("T")


class SqlalchemyDsn(AnyUrl):
    _constraints = UrlConstraints(
        allowed_schemes=[
            "sqlite+aiosqlite",
            "mysql+aiomysql",
            # The real scheme is with an underscore, (oracle+oracledb_async)
            # but pydantic does not validate it, so we use this hack
            "oracle+oracledb-async",
        ]
    )


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


LocalFileUrl = Annotated[FileUrl, BeforeValidator(_apply_default_scheme)]


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


class AuthSettings(ServiceSettingsBase):
    """Settings for the authentication service."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_SERVICE_AUTH_")

    dirac_client_id: str = "myDIRACClientID"
    # TODO: This should be taken dynamically
    # ["http://pclhcb211:8000/docs/oauth2-redirect"]
    allowed_redirects: list[str] = []
    device_flow_expiration_seconds: int = 600
    authorization_flow_expiration_seconds: int = 300

    # State key is used to encrypt/decrypt the state dict passed to the IAM
    state_key: FernetKey

    # TODO: this should probably be something mandatory
    # to set by the user
    token_issuer: str = "http://lhcbdirac.cern.ch/"  # noqa: S105
    token_key: TokenSigningKey
    token_algorithm: str = "RS256"  # noqa: S105
    access_token_expire_minutes: int = 20
    refresh_token_expire_minutes: int = 60

    available_properties: set[SecurityProperty] = Field(
        default_factory=SecurityProperty.available_properties
    )
