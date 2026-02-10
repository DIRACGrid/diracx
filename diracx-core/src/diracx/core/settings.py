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
    """Apply the default file:// scheme if not present."""
    if "://" not in value:
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
        """Context manager to run code at startup and shutdown."""
        yield


class DevelopmentSettings(ServiceSettingsBase):
    """Settings for the Development Configuration that can influence run time."""

    model_config = SettingsConfigDict(
        env_prefix="DIRACX_DEV_", use_attribute_docstrings=True
    )

    crash_on_missed_access_policy: bool = False
    """When set to true (only for demo/CI), crash if an access policy isn't called.

    This is useful for development and testing to ensure all endpoints have proper
    access control policies defined.
    """

    @classmethod
    def create(cls) -> Self:
        return cls()


class AuthSettings(ServiceSettingsBase):
    """Settings for the authentication service."""

    model_config = SettingsConfigDict(
        env_prefix="DIRACX_SERVICE_AUTH_", use_attribute_docstrings=True
    )

    dirac_client_id: str = "myDIRACClientID"
    """OAuth2 client identifier for DIRAC services.

    This should match the client ID registered with the identity provider.
    """

    allowed_redirects: list[str] = []
    """List of allowed redirect URLs for OAuth2 authorization flow.

    These URLs must be pre-registered and should match the redirect URIs
    configured in the OAuth2 client registration.
    Example: ["http://localhost:8000/docs/oauth2-redirect"]
    """

    device_flow_expiration_seconds: int = 600
    """Expiration time in seconds for device flow authorization requests.

    After this time, the device code becomes invalid and users must restart
    the device flow process. Default: 10 minutes.
    """

    authorization_flow_expiration_seconds: int = 300
    """Expiration time in seconds for authorization code flow.

    The time window during which the authorization code remains valid
    before it must be exchanged for tokens. Default: 5 minutes.
    """

    state_key: FernetKey
    """Encryption key used to encrypt/decrypt the state parameter passed to the IAM.

    This key ensures the integrity and confidentiality of state information
    during OAuth2 flows. Must be a valid Fernet key.
    """

    token_issuer: str
    """The issuer identifier for JWT tokens.

    This should be a URI that uniquely identifies the token issuer and
    matches the 'iss' claim in issued JWT tokens.
    """

    token_keystore: TokenSigningKeyStore
    """Keystore containing the cryptographic keys used for signing JWT tokens.

    This includes both public and private keys for token signature
    generation and verification.
    """

    # TODO: EdDSA should be removed later due to "SecurityWarning: EdDSA is deprecated via RFC 9864"
    token_allowed_algorithms: list[str] = ["RS256", "EdDSA", "Ed25519"]  # noqa: S105
    """List of allowed cryptographic algorithms for JWT token signing.

    Supported algorithms include RS256 (RSA with SHA-256) and Ed25519
    (Edwards-curve Digital Signature Algorithm). Default: ["RS256", "Ed25519"]
    """

    access_token_expire_minutes: int = 20
    """Expiration time in minutes for access tokens.

    After this duration, access tokens become invalid and must be refreshed
    or re-obtained. Default: 20 minutes.
    """

    refresh_token_expire_minutes: int = 60
    """Expiration time in minutes for refresh tokens.

    The maximum lifetime of refresh tokens before they must be re-issued
    through a new authentication flow. Default: 60 minutes.
    """

    available_properties: set[SecurityProperty] = Field(
        default_factory=SecurityProperty.available_properties
    )
    """Set of security properties available in this DIRAC installation.

    These properties define various authorization capabilities and are used
    for access control decisions. Defaults to all available security properties.
    """


class SandboxStoreSettings(ServiceSettingsBase):
    """Settings for the sandbox store."""

    model_config = SettingsConfigDict(
        env_prefix="DIRACX_SANDBOX_STORE_", use_attribute_docstrings=True
    )

    bucket_name: str
    """Name of the S3 bucket used for storing job sandboxes.

    This bucket will contain input and output sandbox files for DIRAC jobs.
    The bucket must exist or auto_create_bucket must be enabled.
    """

    s3_client_kwargs: dict[str, Any]
    """Configuration parameters passed to the S3 client."""

    auto_create_bucket: bool = False
    """Whether to automatically create the S3 bucket if it doesn't exist."""

    url_validity_seconds: int = 5 * 60
    """Validity duration in seconds for pre-signed S3 URLs.

    This determines how long generated download/upload URLs remain valid
    before expiring. Default: 300 seconds (5 minutes).
    """

    se_name: str = "SandboxSE"
    """Logical name of the Storage Element for the sandbox store.

    This name is used within DIRAC to refer to this sandbox storage
    endpoint in job descriptions and file catalogs.
    """
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
