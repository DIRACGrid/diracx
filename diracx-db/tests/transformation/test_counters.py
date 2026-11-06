"""Tests for the journalled counter operations (DX-ADR-009).

All tests run against SQLite, which verifies correctness but not concurrency
(SQLite serialises writes natively and has no FOR UPDATE SKIP LOCKED).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import BigInteger, Column, MetaData, String, Table, event, func, insert, select
from sqlalchemy.ext.asyncio import create_async_engine

from diracx.db.sql.utils.counters import (
    CountedTable,
    JournalledCounter,
    counter_locks_table,
    journalled_counter,
)


@pytest.fixture
def simple_metadata():
    """A standalone metadata with a counted table and a single-measure counter."""
    meta = MetaData()
    Table(
        "Items",
        meta,
        Column("ItemID", BigInteger, primary_key=True, autoincrement=True),
        Column("GroupID", BigInteger, nullable=False),
        Column("Status", String(32), nullable=False),
        Column("Size", BigInteger, nullable=False, server_default="0"),
    )
    journalled_counter(
        meta,
        "Item",
        key=[("GroupID", BigInteger()), ("Status", String(32))],
        measures=["Count", "Bytes"],
    )
    return meta


@pytest.fixture
async def engine(simple_metadata):
    eng = create_async_engine("sqlite+aiosqlite:///:memory:")
    event.listen(
        eng.sync_engine,
        "connect",
        lambda dbapi_conn, _: dbapi_conn.cursor().execute("PRAGMA foreign_keys=ON"),
    )
    async with eng.begin() as conn:
        await conn.run_sync(simple_metadata.create_all)
        locks = counter_locks_table(simple_metadata)
        from diracx.db.sql.utils.counters import journalled_counters

        for counter in journalled_counters(simple_metadata):
            from sqlalchemy import insert as sa_insert

            await conn.execute(
                sa_insert(locks).prefix_with("OR IGNORE", dialect="sqlite"),
                counter.lock_rows(),
            )
    yield eng
    await eng.dispose()


@pytest.fixture
def counter(simple_metadata) -> JournalledCounter:
    from diracx.db.sql.utils.counters import journalled_counters

    return journalled_counters(simple_metadata)[0]


@pytest.fixture
def items_table(simple_metadata) -> Table:
    return simple_metadata.tables["Items"]


async def test_journal_and_read_basic(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
            ])

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result == {
        (1, "New"): (5, 100),
        (1, "Done"): (2, 40),
    }


async def test_read_empty(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    assert rows == []


async def test_read_combines_folded_and_unfolded(engine, counter):
    ct = counter.counters_table
    now = datetime.now(tz=timezone.utc)

    # Seed a counter row directly (simulating a prior fold)
    async with engine.connect() as conn:
        async with conn.begin():
            await conn.execute(
                insert(ct).values(GroupID=1, Status="New", Count=10, Bytes=200, UpdatedAt=now)
            )
            # Add unfolded deltas
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 3, "BytesDelta": 50},
            ])

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result == {(1, "New"): (13, 250)}


async def test_fold_moves_journal_to_counters(engine, counter):
    jt = counter.journal_table
    ct = counter.counters_table

    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
            ])

    n = await counter.fold(engine, batch_size=100)
    assert n == 2

    async with engine.connect() as conn:
        async with conn.begin():
            journal_count = (await conn.execute(select(func.count()).select_from(jt))).scalar()
            assert journal_count == 0

            counter_rows = (await conn.execute(select(ct))).all()
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in counter_rows}
    assert result == {
        (1, "New"): (5, 100),
        (1, "Done"): (2, 40),
    }


async def test_fold_upserts_existing_counters(engine, counter):
    ct = counter.counters_table
    now = datetime.now(tz=timezone.utc)

    async with engine.connect() as conn:
        async with conn.begin():
            await conn.execute(
                insert(ct).values(GroupID=1, Status="New", Count=10, Bytes=200, UpdatedAt=now)
            )

    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 3, "BytesDelta": 50},
            ])

    await counter.fold(engine)

    async with engine.connect() as conn:
        async with conn.begin():
            rows = (await conn.execute(select(ct))).all()
    assert len(rows) == 1
    assert rows[0].Count == 13
    assert rows[0].Bytes == 250


async def test_fold_returns_count_and_zero_when_empty(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 1, "BytesDelta": 10},
            ])

    n = await counter.fold(engine)
    assert n == 1

    n = await counter.fold(engine)
    assert n == 0


async def test_fold_batch_size(engine, counter):
    jt = counter.journal_table
    deltas = [
        {"GroupID": i, "Status": "New", "CountDelta": 1, "BytesDelta": 10}
        for i in range(10)
    ]
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, deltas)

    n = await counter.fold(engine, batch_size=3)
    assert n == 3

    async with engine.connect() as conn:
        async with conn.begin():
            remaining = (await conn.execute(select(func.count()).select_from(jt))).scalar()
    assert remaining == 7


async def test_read_exact_across_fold(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                {"GroupID": 2, "Status": "Done", "CountDelta": 3, "BytesDelta": 60},
            ])

    async with engine.connect() as conn:
        async with conn.begin():
            before = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in await counter.read(conn)}

    await counter.fold(engine)

    async with engine.connect() as conn:
        async with conn.begin():
            after = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in await counter.read(conn)}

    assert before == after


async def test_journal_rollback(engine, counter):
    jt = counter.journal_table

    try:
        async with engine.connect() as conn:
            async with conn.begin():
                await counter.journal(conn, [
                    {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                ])
                raise RuntimeError("force rollback")
    except RuntimeError:
        pass

    async with engine.connect() as conn:
        async with conn.begin():
            count = (await conn.execute(select(func.count()).select_from(jt))).scalar()
    assert count == 0


async def test_rebuild_corrects_drift(engine, counter, items_table):
    ct = counter.counters_table
    now = datetime.now(tz=timezone.utc)

    # Insert actual counted rows
    async with engine.connect() as conn:
        async with conn.begin():
            await conn.execute(insert(items_table).values([
                {"ItemID": 1, "GroupID": 1, "Status": "New", "Size": 10},
                {"ItemID": 2, "GroupID": 1, "Status": "New", "Size": 20},
                {"ItemID": 3, "GroupID": 1, "Status": "Done", "Size": 30},
            ]))
            # Seed a wrong counter row (drift)
            await conn.execute(
                insert(ct).values(GroupID=1, Status="New", Count=99, Bytes=999, UpdatedAt=now)
            )

    counted = [CountedTable(
        table=items_table,
        key_exprs=[items_table.c.GroupID, items_table.c.Status],
        measure_exprs={"Count": func.count(), "Bytes": func.sum(items_table.c.Size)},
    )]

    n = await counter.rebuild(engine, counted)
    assert n > 0

    # Fold the corrections
    await counter.fold(engine)

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result == {
        (1, "New"): (2, 30),
        (1, "Done"): (1, 30),
    }


async def test_rebuild_no_correction_when_accurate(engine, counter, items_table):
    ct = counter.counters_table
    now = datetime.now(tz=timezone.utc)

    async with engine.connect() as conn:
        async with conn.begin():
            await conn.execute(insert(items_table).values([
                {"ItemID": 1, "GroupID": 1, "Status": "New", "Size": 10},
            ]))
            await conn.execute(
                insert(ct).values(GroupID=1, Status="New", Count=1, Bytes=10, UpdatedAt=now)
            )

    counted = [CountedTable(
        table=items_table,
        key_exprs=[items_table.c.GroupID, items_table.c.Status],
        measure_exprs={"Count": func.count(), "Bytes": func.sum(items_table.c.Size)},
    )]

    n = await counter.rebuild(engine, counted)
    assert n == 0


async def test_delete_scope(engine, counter):
    ct = counter.counters_table
    jt = counter.journal_table

    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                {"GroupID": 2, "Status": "New", "CountDelta": 3, "BytesDelta": 60},
            ])

    await counter.fold(engine)

    # Add more journal rows for group 1
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
            ])

    await counter.delete_scope(engine, ct.c.GroupID == 1)

    async with engine.connect() as conn:
        async with conn.begin():
            counter_rows = (await conn.execute(select(ct))).all()
            journal_rows = (await conn.execute(select(jt))).all()

    assert len(counter_rows) == 1
    assert counter_rows[0].GroupID == 2

    assert len(journal_rows) == 0


async def test_negative_deltas(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            # Start with some items in New
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
            ])

    await counter.fold(engine)

    # Transition: move 2 items from New to Done
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": -2, "BytesDelta": -40},
                {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
            ])

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result == {
        (1, "New"): (3, 60),
        (1, "Done"): (2, 40),
    }


async def test_read_with_where(engine, counter):
    ct = counter.counters_table

    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(conn, [
                {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                {"GroupID": 2, "Status": "New", "CountDelta": 3, "BytesDelta": 60},
                {"GroupID": 1, "Status": "Done", "CountDelta": 1, "BytesDelta": 10},
            ])

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn, where=ct.c.GroupID == 1)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result == {
        (1, "New"): (5, 100),
        (1, "Done"): (1, 10),
    }


async def test_rebuild_scoped(engine, counter, items_table):
    ct = counter.counters_table
    now = datetime.now(tz=timezone.utc)

    async with engine.connect() as conn:
        async with conn.begin():
            await conn.execute(insert(items_table).values([
                {"ItemID": 1, "GroupID": 1, "Status": "New", "Size": 10},
                {"ItemID": 2, "GroupID": 2, "Status": "New", "Size": 20},
            ]))
            # Wrong counter for group 1 only
            await conn.execute(
                insert(ct).values(GroupID=1, Status="New", Count=99, Bytes=999, UpdatedAt=now)
            )
            await conn.execute(
                insert(ct).values(GroupID=2, Status="New", Count=1, Bytes=20, UpdatedAt=now)
            )

    counted = [CountedTable(
        table=items_table,
        key_exprs=[items_table.c.GroupID, items_table.c.Status],
        measure_exprs={"Count": func.count(), "Bytes": func.sum(items_table.c.Size)},
    )]

    n = await counter.rebuild(engine, counted, where=ct.c.GroupID == 1)
    assert n > 0

    await counter.fold(engine)

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result[(1, "New")] == (1, 10)
    # Group 2 untouched
    assert result[(2, "New")] == (1, 20)
