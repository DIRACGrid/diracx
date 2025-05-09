"""Settings for the core services."""

from __future__ import annotations

import json

from diracx.core.properties import SecurityProperty
from diracx.core.s3 import s3_bucket_exists

__all__ = (
    "SqlalchemyDsn",
    "LocalFileUrl",
    "ServiceSettingsBase",
)

import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Self, TypeVar, cast

from aiobotocore.session import get_session
from botocore.config import Config
from botocore.errorfactory import ClientError
from cryptography.fernet import Fernet
from joserfc.jwk import KeySet, KeySetSerialization
from pydantic import (
    AnyUrl,
    BeforeValidator,
    Field,
    FileUrl,
    PrivateAttr,
    SecretStr,
    TypeAdapter,
    UrlConstraints,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

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


class _TokenSigningKeyStore(SecretStr):
    jwks: KeySet

    def __init__(self, data: str):
        super().__init__(data)

        # Load the keys from the JSON string
        try:
            keys = json.loads(self.get_secret_value())
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON string") from e
        if not isinstance(keys, dict):
            raise ValueError("Invalid JSON string")
        if "keys" not in keys:
            raise ValueError("Invalid JSON string, missing 'keys' field")
        if not isinstance(keys["keys"], list):
            raise ValueError("Invalid JSON string, 'keys' field must be a list")
        if not keys["keys"]:
            raise ValueError("Invalid JSON string, 'keys' field is empty")

        self.jwks = KeySet.import_key_set(cast(KeySetSerialization, keys))


def _maybe_load_keys_from_file(value: Any) -> Any:
    """Load jwks from files if needed."""
    if isinstance(value, str):
        # If the value is a string, we need to check if it is a JSON string or a file URL
        if not (value.strip().startswith("{") or value.startswith("[")):
            # If it is not a JSON string, we assume it is a file URL
            url = TypeAdapter(LocalFileUrl).validate_python(value)
            if not url.scheme == "file":
                raise ValueError("Only file:// URLs are supported")
            if url.path is None:
                raise ValueError("No path specified")
            return Path(url.path).read_text()

    return value


TokenSigningKeyStore = Annotated[
    _TokenSigningKeyStore,
    BeforeValidator(_maybe_load_keys_from_file),
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

    @classmethod
    def create(cls) -> Self:
        return cls()


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

    token_issuer: str
    token_keystore: TokenSigningKeyStore
    token_allowed_algorithms: list[str] = ["RS256", "EdDSA"]  # noqa: S105
    access_token_expire_minutes: int = 20
    refresh_token_expire_minutes: int = 60

    available_properties: set[SecurityProperty] = Field(
        default_factory=SecurityProperty.available_properties
    )


class SandboxStoreSettings(ServiceSettingsBase):
    """Settings for the sandbox store."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_SANDBOX_STORE_")

    bucket_name: str
    s3_client_kwargs: dict[str, str]
    auto_create_bucket: bool = False
    url_validity_seconds: int = 5 * 60
    se_name: str = "SandboxSE"
    _client: S3Client = PrivateAttr()

    @contextlib.asynccontextmanager
    async def lifetime_function(self) -> AsyncIterator[None]:
        async with get_session().create_client(
            "s3",
            **self.s3_client_kwargs,
            config=Config(signature_version="v4"),
        ) as self._client:  # type: ignore
            if not await s3_bucket_exists(self._client, self.bucket_name):
                if not self.auto_create_bucket:
                    raise ValueError(
                        f"Bucket {self.bucket_name} does not exist and auto_create_bucket is disabled"
                    )
                try:
                    await self._client.create_bucket(Bucket=self.bucket_name)
                except ClientError as e:
                    raise ValueError(
                        f"Failed to create bucket {self.bucket_name}"
                    ) from e

            yield

    @property
    def s3_client(self) -> S3Client:
        if self._client is None:
            raise RuntimeError("S3 client accessed before lifetime function")
        return self._client
