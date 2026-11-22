"""Checks on the MySQL driver stack which don't need a running server."""

from __future__ import annotations

from aiomysql.connection import Connection


async def test_aiomysql_escapes_bytes():
    """Bytes parameters must be escapable by the installed aiomysql/PyMySQL pair.

    aiomysql 0.3.2 relies on private PyMySQL helpers which were removed in
    PyMySQL 1.2.1 (aio-libs/aiomysql#1080), so this fails if the cap on pymysql
    in pyproject.toml is lifted before aiomysql is fixed.
    """
    # The constructor doesn't open a connection
    conn = Connection(host="db.invalid", user="user", password="pass", db="name")
    assert conn.escape(b"\x00'abc") == "_binary'\\0\\'abc'"
