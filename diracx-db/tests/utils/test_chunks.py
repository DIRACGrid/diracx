from __future__ import annotations

import uuid

import pytest
import uuid_utils
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    event,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.ext.asyncio import create_async_engine

from diracx.db.sql.utils import execute_in_chunks, fetch_in_chunks

metadata = MetaData()
items = Table(
    "Items",
    metadata,
    Column("ItemID", Integer, primary_key=True),
    Column("Label", String(32)),
)


@pytest.fixture
async def conn():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    statements: list[str] = []

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            insert(items), [{"ItemID": i, "Label": f"item{i}"} for i in range(10)]
        )
        statements.clear()
        connection.info["statements"] = statements
        yield connection
    await engine.dispose()


async def test_fetch_in_chunks(conn):
    rows = await fetch_in_chunks(
        conn,
        lambda chunk: (
            select(items.c.ItemID)
            .where(items.c.ItemID.in_(chunk))
            .order_by(items.c.ItemID)
        ),
        [7, 1, 3, 1, 42, 9, 7],
        chunk_size=2,
    )
    # Duplicates are dropped and the rest sorted: [1, 3], [7, 9], [42]
    assert len(conn.info["statements"]) == 3
    # Missing values give no row
    assert [row.ItemID for row in rows] == [1, 3, 7, 9]


async def test_chunks_are_sorted(conn):
    # Concurrent transactions only avoid deadlocks if every one of them takes
    # its row locks in the same order, whatever order the values come in
    seen: list[list[int]] = []

    def build(chunk):
        seen.append(chunk)
        return update(items).where(items.c.ItemID.in_(chunk)).values(Label="x")

    await execute_in_chunks(conn, build, [9, 0, 5, 3, 9, 7, 1], chunk_size=3)
    assert seen == [[0, 1, 3], [5, 7, 9]]


async def test_fetch_in_chunks_empty(conn):
    assert await fetch_in_chunks(conn, lambda chunk: select(items), []) == []
    assert conn.info["statements"] == []


async def test_execute_in_chunks_update(conn):
    rowcount = await execute_in_chunks(
        conn,
        lambda chunk: (
            update(items).where(items.c.ItemID.in_(chunk)).values(Label="updated")
        ),
        [0, 2, 4, 6, 8, 100],
        chunk_size=4,
    )
    assert rowcount == 5
    assert len(conn.info["statements"]) == 2
    updated = await conn.scalar(
        select(func.count()).select_from(items).where(items.c.Label == "updated")
    )
    assert updated == 5


async def test_execute_in_chunks_delete(conn):
    rowcount = await execute_in_chunks(
        conn,
        lambda chunk: delete(items).where(items.c.ItemID.in_(chunk)),
        iter(range(5)),
        chunk_size=3,
    )
    assert rowcount == 5
    assert await conn.scalar(select(func.count()).select_from(items)) == 5


async def test_execute_in_chunks_empty(conn):
    assert await execute_in_chunks(conn, lambda chunk: delete(items), []) == 0
    assert conn.info["statements"] == []


async def test_chunks_sort_mixed_uuid_classes(conn):
    # uuid.UUID and uuid_utils.UUID cannot be compared with each other, so
    # they are sorted by their bytes, as a BINARY(16) column sorts them
    values = [uuid_utils.uuid4(), uuid.uuid4(), uuid_utils.uuid4(), uuid.uuid4()]
    seen: list[list] = []

    def build(chunk):
        seen.append(chunk)
        return delete(items).where(items.c.ItemID == -1)

    await execute_in_chunks(conn, build, values, chunk_size=3)
    assert [v.bytes for chunk in seen for v in chunk] == sorted(v.bytes for v in values)
