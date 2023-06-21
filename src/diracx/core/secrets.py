from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from authlib.jose import JsonWebKey
from pydantic import AnyUrl, SecretStr, parse_obj_as

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
