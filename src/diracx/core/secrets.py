from __future__ import annotations

__all__ = ("get_secrets",)

from typing import Literal

from authlib.jose import JsonWebKey
from cachetools import LRUCache, cached
from pydantic import AnyUrl, BaseSettings, Field, SecretStr

from .utils import dotenv_files_from_environment


class RawSqlalchemyDsn(AnyUrl):
    allowed_schemes = {"sqlite+aiosqlite", "mysql+aiomysql"}


SqlalchemyDsn = Literal["sqlite+aiosqlite:///:memory:"] | RawSqlalchemyDsn


class DbUrls(BaseSettings, env_prefix="DIRACX_SECRET_DB_URL_"):
    auth: SqlalchemyDsn
    jobs: SqlalchemyDsn | None = None


class TokenSigningKey(SecretStr):
    jwk: JsonWebKey

    def __init__(self, data: str):
        super().__init__(data)
        self.jwk = JsonWebKey.import_key(self.get_secret_value())


class ConfigUrl(AnyUrl):
    host_required = False
    allowed_schemes = {"file"}


class DiracxSecrets(BaseSettings, env_prefix="DIRACX_SECRET_", allow_mutation=False):
    config: ConfigUrl
    token_key: TokenSigningKey
    db_url: DbUrls = Field(default_factory=DbUrls)

    @classmethod
    def from_env(cls):
        return cls(_env_file=dotenv_files_from_environment("DIRACX_SECRET_DOTENV"))


@cached(cache=LRUCache(maxsize=1))
def get_secrets() -> DiracxSecrets:
    return DiracxSecrets.from_env()
