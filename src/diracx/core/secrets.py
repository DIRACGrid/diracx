from __future__ import annotations

__all__ = ("get_secrets",)

from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

from authlib.jose import JsonWebKey
from cachetools import LRUCache, cached
from pydantic import AnyUrl, BaseSettings, Field, PrivateAttr, SecretStr, parse_obj_as

from diracx.db import AuthDB, JobDB

if TYPE_CHECKING:
    from pydantic.config import BaseConfig
    from pydantic.fields import ModelField


class RawSqlalchemyDsn(AnyUrl):
    allowed_schemes = {"sqlite+aiosqlite", "mysql+aiomysql"}


SqlalchemyDsn = Literal["sqlite+aiosqlite:///:memory:"] | RawSqlalchemyDsn


class TokenSigningKey(SecretStr):
    jwk: JsonWebKey

    def __init__(self, data: str):
        super().__init__(data)
        self.jwk = JsonWebKey.import_key(self.get_secret_value())

    @classmethod
    # TODO: This should return TokenSigningKey but pydantic's type hints are wrong
    def validate(cls, value: Any) -> SecretStr:
        """Load private keys from files if needed"""
        if isinstance(value, str) and not value.strip().startswith("-----BEGIN"):
            url = parse_obj_as(LocalFileUrl, value)
            value = Path(url.path).read_text()
        return super().validate(value)


class LocalFileUrl(AnyUrl):
    host_required = False
    allowed_schemes = {"file"}

    @classmethod
    # TODO: This should return LocalFileUrl but pydantic's type hints are wrong
    def validate(cls, value: Any, field: ModelField, config: BaseConfig) -> AnyUrl:
        """Overrides AnyUrl.validate to add file:// scheme if not present."""
        if isinstance(value, str) and "://" not in value:
            value = f"file://{value}"
        return super().validate(value, field, config)


class AuthSecrets(BaseSettings, env_prefix="DIRACX_SECRET_AUTH_"):
    db_url: SqlalchemyDsn
    db: Annotated[AuthDB, PrivateAttr()]
    token_issuer: str = "http://lhcbdirac.cern.ch/"
    token_audience: str = "dirac"
    token_key: TokenSigningKey
    token_algorithm: str = "RS256"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, db=AuthDB(None))
        self.db._db_url = self.db_url


class ConfigSecrets(BaseSettings, env_prefix="DIRACX_SECRET_CONFIG_"):
    backend_url: LocalFileUrl


class JobsSecrets(BaseSettings, env_prefix="DIRACX_SECRET_JOBS_"):
    db_url: SqlalchemyDsn
    db: Annotated[JobDB, PrivateAttr()]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, db=JobDB(None))
        self.db._db_url = self.db_url


class DiracxSecrets(BaseSettings, env_prefix="DIRACX_SECRET_", allow_mutation=False):
    auth: AuthSecrets | None = Field(default_factory=AuthSecrets)
    config: ConfigSecrets | None = Field(default_factory=ConfigSecrets)
    jobs: JobsSecrets | None = Field(default_factory=JobsSecrets)


@cached(cache=LRUCache(maxsize=1))
def get_secrets() -> DiracxSecrets:
    return DiracxSecrets()
