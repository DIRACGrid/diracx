"""Settings for the core services."""

from __future__ import annotations

__all__ = [
    "AuthSettings",
    "DevelopmentSettings",
    "FactorySettings",
    "LocalFileUrl",
    "LoggingSettings",
    "OTELSettings",
    "SandboxStoreSettings",
    "ServiceSettingsBase",
    "SqlalchemyDsn",
    "TokenSigningKeyStore",
]

import contextlib
import json
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Annotated, Any, Literal, Self, TypeVar, cast

import dotenv
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
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from signurlarity.aio.client import AsyncClient
from signurlarity.exceptions import SignurlarityError

from .config.sources import ConfigSourceUrl
from .extensions import DiracEntryPoint, select_from_extension
from .properties import SecurityProperty
from .s3 import s3_bucket_exists
from .utils import dotenv_files_from_environment

T = TypeVar("T")


class SqlalchemyDsn(AnyUrl):
    """Database URL type for supported asynchronous SQL drivers."""

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
    """Secret string wrapper containing an imported signing key set."""

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
    """Load a JWKS from a file URL when needed.

    Args:
        value: JWKS JSON data or a file URL containing the data.

    Returns:
        The loaded JWKS text, or the original value when no loading is needed.
    """
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
    """Secret string wrapper containing a Fernet encryption key."""

    fernet: Fernet

    def __init__(self, data: str):
        super().__init__(data)
        self.fernet = Fernet(self.get_secret_value())


def _apply_default_scheme(value: str) -> str:
    """Apply the default ``file://`` scheme if not present.

    Args:
        value: File path or URL to normalize.

    Returns:
        The value with a ``file://`` scheme when one was not provided.
    """
    if "://" not in value:
        value = f"file://{value}"
    return value


LocalFileUrl = Annotated[FileUrl, BeforeValidator(_apply_default_scheme)]


class ServiceSettingsBase(BaseSettings):
    """Base class for service-specific settings."""

    model_config = SettingsConfigDict(frozen=True)

    @classmethod
    def create(cls) -> Self:
        """Create service settings.

        Returns:
            A service settings instance.

        Raises:
            NotImplementedError: Always, because subclasses must implement this method.
        """
        raise NotImplementedError("This should never be called")

    @contextlib.asynccontextmanager
    async def lifetime_function(self) -> AsyncIterator[None]:
        """Run service startup and shutdown handling around a context.

        Yields:
            ``None`` while the service is running.
        """
        yield


class DevelopmentSettings(ServiceSettingsBase):
    """Settings for the Development Configuration that can influence run time.

    Attributes:
        crash_on_missed_access_policy: Whether to fail when an access policy is missed.
    """

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
        """Create development settings from the current environment.

        Returns:
            The development settings instance.
        """
        return cls()


class AuthSettings(ServiceSettingsBase):
    """Settings for the authentication service.

    Attributes:
        dirac_client_id: OAuth2 client identifier for DIRAC clients.
        allowed_redirects: Redirect URLs allowed during authorization.
        device_flow_expiration_seconds: Device flow expiration time in seconds.
        authorization_flow_expiration_seconds: Authorization code expiration time in seconds.
        completed_flow_retention_minutes: Retention time for completed flows in minutes.
        state_key: Key used to encrypt and decrypt OAuth2 state values.
        token_issuer: Issuer identifier for JWT tokens.
        token_keystore: Cryptographic keys used to sign and verify JWTs.
        token_allowed_algorithms: Algorithms allowed for JWT signing.
        access_token_expire_minutes: Access token lifetime in minutes.
        refresh_token_expire_minutes: Refresh token lifetime in minutes.
        refresh_token_retention_months: Refresh token retention period in months.
        available_properties: Security properties available in the installation.
    """

    @model_validator(mode="after")
    def check_retention_greater_than_expiration(self) -> Self:
        """Ensure retention exceeds expiration to avoid deleting valid flows.

        Returns:
            The validated authentication settings.
        """
        if self.completed_flow_retention_minutes <= (
            self.device_flow_expiration_seconds / 60
        ) or self.completed_flow_retention_minutes <= (
            self.authorization_flow_expiration_seconds / 60
        ):
            raise ValueError(
                f"completed_flow_retention_minutes ({self.completed_flow_retention_minutes} minutes) must be bigger"
                f" than device_flow_expiration_seconds ({self.device_flow_expiration_seconds / 60} minutes) and"
                f" authorization_flow_expiration_seconds: ({self.authorization_flow_expiration_seconds / 60} minutes)"
            )
        return self

    model_config = SettingsConfigDict(
        env_prefix="DIRACX_SERVICE_AUTH_", use_attribute_docstrings=True
    )

    dirac_client_id: str = "myDIRACClientID"
    """OAuth2 client identifier for DIRAC clients (cli, web) to DIRAC services.

    There is no real reason to change that.
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

    completed_flow_retention_minutes: int = 60
    """Retention time in minutes for completed flow.

    The maximum retention time of flow after being completed
    and before they are deleted. Default: 60 minutes.
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

    token_allowed_algorithms: list[str] = ["RS256", "Ed25519"]  # noqa: S105
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

    refresh_token_retention_months: int = 6
    """Retention time in months for refresh tokens.

    Refresh tokens live in monthly partitions that are dropped once the whole
    month is older than this many months. It is therefore the longest a refresh
    token (revoked or not) is kept before removal. Default: 6 months.
    """

    available_properties: set[SecurityProperty] = Field(
        default_factory=SecurityProperty.available_properties
    )
    """Set of security properties available in this DIRAC installation.

    These properties define various authorization capabilities and are used
    for access control decisions. Defaults to all available security properties.
    """


class SandboxStoreSettings(ServiceSettingsBase):
    """Settings for the sandbox store.

    Attributes:
        bucket_name: S3 bucket used for job sandboxes.
        s3_client_kwargs: Configuration passed to the S3 client.
        auto_create_bucket: Whether to create a missing S3 bucket.
        url_validity_seconds: Validity period for presigned S3 URLs.
        se_name: Logical Storage Element name for the sandbox store.
        s3_max_pool_connections: Maximum S3 client connection pool size.
        clean_batch_size: Number of candidates selected per cleaning batch.
        clean_delete_chunk_size: Number of database rows deleted per chunk.
        clean_max_concurrent_db_deletes: Maximum concurrent database delete chunks.
    """

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

    s3_max_pool_connections: int = 50
    """Maximum number of connections in the S3 client connection pool.

    Higher values allow more parallel S3 requests (e.g. during bulk sandbox
    deletion).
    """

    clean_batch_size: int = 50_000
    """Number of sandbox candidates to select per batch during cleaning.

    Each batch runs SELECT → S3 delete → DB delete sequentially.
    """

    clean_delete_chunk_size: int = 1000
    """Number of sandbox DB rows to delete per chunk during cleaning.

    Smaller chunks mean shorter transactions and less lock contention.
    """

    clean_max_concurrent_db_deletes: int = 10
    """Maximum number of concurrent DB delete chunks during cleaning.

    Controls parallelism of database DELETE operations.
    """

    _client: AsyncClient = PrivateAttr()

    @contextlib.asynccontextmanager
    async def lifetime_function(self) -> AsyncIterator[None]:
        """Create the S3 client and ensure the configured bucket is available.

        Yields:
            ``None`` while the S3 client is available.
        """
        async with AsyncClient(
            **self.s3_client_kwargs, httpx_max_connections=self.s3_max_pool_connections
        ) as self._client:  # type: ignore
            if not await s3_bucket_exists(self._client, self.bucket_name):
                if not self.auto_create_bucket:
                    raise ValueError(
                        f"Bucket {self.bucket_name} does not exist and auto_create_bucket is disabled"
                    )
                try:
                    await self._client.create_bucket(Bucket=self.bucket_name)
                except SignurlarityError as e:
                    raise ValueError(
                        f"Failed to create bucket {self.bucket_name}"
                    ) from e

            yield

    @property
    def s3_client(self) -> AsyncClient:
        """Return the active S3 client.

        Returns:
            The S3 client created by ``lifetime_function``.

        Raises:
            RuntimeError: If accessed before ``lifetime_function`` starts.
        """
        if self._client is None:
            raise RuntimeError("S3 client accessed before lifetime function")
        return self._client


class LoggingSettings(ServiceSettingsBase):
    """Settings for the logs written by the DiracX processes."""

    model_config = SettingsConfigDict(
        env_prefix="DIRACX_LOG_", use_attribute_docstrings=True
    )

    level: str = "INFO"
    """
    Level of the DiracX loggers (including those of the extension).
    """

    libraries_level: str = "WARNING"
    """
    Level of the loggers of the other libraries (SQLAlchemy, httpx...).
    """

    format: Literal["text", "json"] = "text"
    """
    Format of the logs: ``text`` (human readable) or ``json`` (one JSON object
    per line, for log collectors).
    """

    @field_validator("level", "libraries_level")
    @classmethod
    def validate_level(cls, value: str) -> str:
        """Accept the level names in any case, and reject unknown ones."""
        level = value.upper()
        if level not in logging.getLevelNamesMapping():
            raise ValueError(
                f"Unknown log level {value!r}, expected one of "
                f"{', '.join(logging.getLevelNamesMapping())}"
            )
        return level


class OTELSettings(ServiceSettingsBase):
    """Settings for the Open Telemetry Configuration."""

    model_config = SettingsConfigDict(
        env_prefix="DIRACX_OTEL_", use_attribute_docstrings=True
    )

    enabled: bool = False
    """
    Determines whether OpenTelemetry is enabled.
    """

    application_name: str = "diracx"
    """
    The name of the application for OpenTelemetry.
    """

    protocol: Literal["grpc", "http"] = "grpc"
    """
    The protocol used to send the data to the OpenTelemetry collector:
    OTLP over gRPC (``grpc``, see ``grpc_endpoint``) or over HTTP
    (``http``, protobuf encoded, see ``http_endpoint``).
    """

    grpc_endpoint: str = ""
    """
    The gRPC endpoint for the OpenTelemetry collector (``host:port``,
    e.g. ``otel-collector:4317``), used with the ``grpc`` protocol.
    """

    grpc_insecure: bool = True
    """
    Whether to use an insecure gRPC connection for the OpenTelemetry collector.
    """

    http_endpoint: str = ""
    """
    The base URL of the OpenTelemetry collector (e.g. ``http://otel-collector:4318``),
    used with the ``http`` protocol. ``/v1/traces``, ``/v1/metrics`` and ``/v1/logs``
    are appended to it. The scheme (``http`` or ``https``) decides whether TLS is used.
    """

    headers: dict[str, str] | None = None
    """
    A JSON-encoded dictionary of headers to pass to the OpenTelemetry collector, e.g. {"tenant_id": "lhcbdiracx-cert"}.
    """


class FactorySettings(ServiceSettingsBase):
    """Factory settings.

    Settings which do not fit into dedicated classes,
    or are dynamically generated.

    Attributes:
        config_backend_url: URL of the configuration backend.
        legacy_exchange_hashed_api_key: Hashed API key for legacy exchange.
        tasks_redis_url: URL of the Redis server used for tasks.
        enabled_services: Map of service names to enabled states.
        opensearch_dbs: OpenSearch database connection URLs.
        sql_dbs: SQL database connection URLs.
    """

    # We want to be able to read both from specific environment variables
    # but also to create the object directly with the attribute name
    # https://pydantic.dev/docs/validation/latest/concepts/alias#validation
    model_config = SettingsConfigDict(
        use_attribute_docstrings=True, validate_by_alias=True, validate_by_name=True
    )

    config_backend_url: ConfigSourceUrl | None = Field(
        default=None,
        validation_alias="DIRACX_CONFIG_BACKEND_URL",
    )
    """The URL of the configuration backend.
    """

    legacy_exchange_hashed_api_key: str = Field(
        default="", validation_alias="DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY"
    )
    """The hashed API key for the legacy exchange endpoint.
    """

    tasks_redis_url: str = Field(
        default="redis://localhost", validation_alias="DIRACX_TASKS_REDIS_URL"
    )
    """The url for the redis server to manage tasks"""

    os_global_prefix: str = Field(
        default="", validation_alias="DIRACX_FACTORY_OS_GLOBAL_PREFIX"
    )
    """Global prefix for OpenSearch database indices."""

    enabled_services: dict[str, bool] = Field(default_factory=dict)
    """The following environment variables dictates which routers are enabled."""

    opensearch_dbs: dict[str, str] = Field(default_factory=dict)
    """The following environment variables configure the OpenSearch database connections."""

    sql_dbs: dict[str, str] = Field(default_factory=dict)
    """The following environment variables configure the SQL database connections."""

    @model_validator(mode="before")
    @classmethod
    def load_dotenv_files(cls, data: Any) -> Any:
        """Load dotenv files before reading settings from the environment.

        Args:
            data: Raw settings data.

        Returns:
            The unchanged settings data after dotenv files are loaded.
        """
        for env_file in dotenv_files_from_environment("DIRACX_SERVICE_DOTENV"):
            if not dotenv.load_dotenv(env_file):
                raise NotImplementedError(f"Could not load dotenv file {env_file}")
        return data

    @field_validator("enabled_services", mode="before")
    @classmethod
    def build_enabled_services(cls, value: Any) -> dict[str, bool]:
        """Build enabled services from installed service entry points.

        Args:
            value: Explicit service enablement values.

        Returns:
            Service enablement values merged with environment settings.
        """
        enabled_services: dict[str, bool] = {
            entry_point.name: True
            for entry_point in select_from_extension(group=DiracEntryPoint.SERVICES)
            if "well-known" not in entry_point.name
        }

        for service_name in enabled_services:
            env_name = f"DIRACX_SERVICE_{service_name.upper()}_ENABLED"
            if env_value := os.environ.get(env_name):
                enabled_services[service_name] = TypeAdapter(bool).validate_python(
                    env_value
                )

        if isinstance(value, dict):
            enabled_services.update(value)
        return enabled_services

    @field_validator("opensearch_dbs", mode="before")
    @classmethod
    def build_opensearch_dbs(cls, value: Any) -> dict[str, str]:
        """Build OpenSearch database URLs from installed entry points.

        Args:
            value: Explicit OpenSearch database URLs.

        Returns:
            Database URLs merged with environment settings.
        """
        opensearch_dbs: dict[str, str] = {
            entry_point.name: ""
            for entry_point in select_from_extension(group=DiracEntryPoint.OS_DB)
        }

        for db_name in opensearch_dbs:
            env_name = f"DIRACX_OS_DB_{db_name.upper()}"
            if env_value := os.environ.get(env_name):
                opensearch_dbs[db_name] = env_value

        if isinstance(value, dict):
            opensearch_dbs.update(value)
        return opensearch_dbs

    @field_validator("sql_dbs", mode="before")
    @classmethod
    def build_sql_dbs(cls, value: Any) -> dict[str, str]:
        """Build SQL database URLs from installed entry points.

        Args:
            value: Explicit SQL database URLs.

        Returns:
            Database URLs merged with environment settings.
        """
        sql_dbs: dict[str, str] = {
            entry_point.name: ""
            for entry_point in select_from_extension(group=DiracEntryPoint.SQL_DB)
        }

        for db_name in sql_dbs:
            env_name = f"DIRACX_DB_URL_{db_name.upper()}"
            if env_value := os.environ.get(env_name):
                sql_dbs[db_name] = env_value

        if isinstance(value, dict):
            sql_dbs.update(value)
        return sql_dbs
