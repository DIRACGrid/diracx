from __future__ import annotations

import uuid

import pytest
from sqlalchemy import Uuid
from sqlalchemy.dialects.mysql import aiomysql

from diracx.db.sql.auth.schema import AuthorizationFlows, RefreshTokens


def mariadb_dialect():
    """A ``mysql+aiomysql`` dialect as set up after connecting to MariaDB 11.4.

    Mirrors what SQLAlchemy does on the first connection when it finds a
    MariaDB server, which enables the native ``UUID`` type from 10.7.
    """
    dialect = aiomysql.dialect()
    dialect._set_mariadb(True, dialect._parse_server_version("11.4.2-MariaDB"))
    dialect.supports_native_uuid = True
    dialect._allows_uuid_binds = True
    return dialect


def test_mariadb_dialect_uses_native_uuid():
    # Without this, the test below would pass whatever the column type is
    assert Uuid(as_uuid=False).compile(dialect=mariadb_dialect()) == "UUID"


@pytest.mark.parametrize(
    "column", [AuthorizationFlows.__table__.c.UUID, RefreshTokens.__table__.c.JTI]
)
def test_uuid_columns_stay_char32_on_mariadb(column):
    dialect = mariadb_dialect()
    assert column.type.compile(dialect=dialect) == "CHAR(32)"
    # Bound as 32 hex digits, matching rows in existing CHAR(32) tables
    value = uuid.uuid4()
    bind = column.type.dialect_impl(dialect).bind_processor(dialect)
    assert bind(str(value)) == value.hex
