from __future__ import annotations

__all__ = (
    "SqlalchemyDsn",
    "LocalFileUrl",
    "ServiceSettingsBase",
)

import contextlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator, Self, TypeVar

from authlib.jose import JsonWebKey
from pydantic import AnyUrl, BaseSettings, SecretStr, parse_obj_as

if TYPE_CHECKING:
    from pydantic.config import BaseConfig
    from pydantic.fields import ModelField


T = TypeVar("T")


class SqlalchemyDsn(AnyUrl):
    allowed_schemes = {"sqlite+aiosqlite", "mysql+aiomysql"}


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


class ServiceSettingsBase(BaseSettings, allow_mutation=False):
    @classmethod
    def create(cls) -> Self:
        raise NotImplementedError("This should never be called")

    @contextlib.asynccontextmanager
    async def lifetime_function(self) -> AsyncIterator[None]:
        """A context manager that can be used to run code at startup and shutdown."""
        yield
