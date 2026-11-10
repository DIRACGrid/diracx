"""Tests for the journalled counter operations (DX-ADR-009).

Tests are parametrised over database backends:

* **SQLite** — always runs (no external dependency).
* **MySQL 8.0** — runs when testcontainers + Docker are available, exercising
  the MySQL-specific branches (isolation levels, ``FOR UPDATE SKIP LOCKED``,
  ``ON DUPLICATE KEY UPDATE``, ``BIGINT`` cast, ``TEMPORARY`` tables,
  ``CounterLocks`` row locks).
* **MariaDB 11.4** — same as MySQL but covers MariaDB-specific behaviour
  (consecutive ``AUTO_INCREMENT`` lock mode, ``VALUES()`` in ``ODKU``).

When Docker is unavailable on a developer laptop the MySQL and MariaDB
tests are *skipped* (visible in ``pytest -v`` output).  In CI, set
``DIRACX_REQUIRE_MYSQL_TESTS=1`` to turn those skips into hard failures.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import (
    BigInteger,
    Column,
    MetaData,
    String,
    Table,
    func,
    insert,
    select,
)
from sqlalchemy import insert as sa_insert
from sqlalchemy.ext.asyncio import create_async_engine
from uuid_utils import uuid7

from diracx.db.sql.transformation.schema import (
    DATA_PARCELS_COUNTER,
    TRANSFORMATION_COUNTER,
    Base,
)
from diracx.db.sql.utils.counters import (
    CountedTable,
    JournalledCounter,
    counter_locks_table,
    journalled_counter,
    journalled_counters,
)

try:
    from testcontainers.community.mysql import MySqlContainer

    HAS_TESTCONTAINERS = True
except ImportError:
    MySqlContainer = None
    HAS_TESTCONTAINERS = False

REQUIRE_MYSQL = os.environ.get("DIRACX_REQUIRE_MYSQL_TESTS", "") == "1"


def _make_async_url(raw_url: str) -> str:
    """Rewrite a testcontainers DBAPI URL to use aiomysql."""
    url = raw_url.replace("mysql://", "mysql+aiomysql://", 1)
    url = url.replace("mysql+pymysql://", "mysql+aiomysql://", 1)
    return url


def _skip_or_fail(reason: str) -> None:
    """Skip, or fail if CI requires MySQL tests."""
    if REQUIRE_MYSQL:
        pytest.fail(f"DIRACX_REQUIRE_MYSQL_TESTS=1 but: {reason}")
    pytest.skip(reason)


@pytest.fixture(scope="session")
def mysql_url():
    """Start a MySQL 8.0.40 container for the test session."""
    if not HAS_TESTCONTAINERS:
        _skip_or_fail("testcontainers not installed")

    try:
        with MySqlContainer("mysql:8.0.40") as mysql:
            yield _make_async_url(mysql.get_connection_url())
    except Exception as e:
        _skip_or_fail(f"Docker unavailable: {e}")


@pytest.fixture(scope="session")
def mariadb_url():
    """Start a MariaDB 11.4.4 container for the test session."""
    if not HAS_TESTCONTAINERS:
        _skip_or_fail("testcontainers not installed")

    try:
        with MySqlContainer(
            "mariadb:11.4.4",
            wait_strategy_check_string=(
                ".*: ready for connections.*: ready for connections.*"
            ),
        ) as maria:
            yield _make_async_url(maria.get_connection_url())
    except Exception as e:
        _skip_or_fail(f"Docker unavailable: {e}")


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


@pytest.fixture(params=["sqlite", "mysql", "mariadb"])
async def engine(request, simple_metadata, mysql_url, mariadb_url):
    backend = request.param

    if backend == "sqlite":
        eng = create_async_engine("sqlite+aiosqlite:///:memory:")
    elif backend == "mysql":
        eng = create_async_engine(mysql_url)
    elif backend == "mariadb":
        eng = create_async_engine(mariadb_url)
    else:
        raise ValueError(f"Unknown backend: {backend}")

    async with eng.begin() as conn:
        await conn.run_sync(simple_metadata.create_all)
        locks = counter_locks_table(simple_metadata)
        for counter in journalled_counters(simple_metadata):
            await conn.execute(
                sa_insert(locks).prefix_with("OR IGNORE", dialect="sqlite"),
                counter.lock_rows(),
            )

    yield eng

    if backend != "sqlite":
        async with eng.begin() as conn:
            await conn.run_sync(simple_metadata.drop_all)
    await eng.dispose()


@pytest.fixture
async def sqlite_engine(simple_metadata):
    """SQLite-only engine for backend-independent tests."""
    eng = create_async_engine("sqlite+aiosqlite:///:memory:")

    async with eng.begin() as conn:
        await conn.run_sync(simple_metadata.create_all)
        locks = counter_locks_table(simple_metadata)
        for counter in journalled_counters(simple_metadata):
            await conn.execute(
                sa_insert(locks).prefix_with("OR IGNORE", dialect="sqlite"),
                counter.lock_rows(),
            )

    yield eng
    await eng.dispose()


@pytest.fixture
def counter(simple_metadata) -> JournalledCounter:
    return journalled_counters(simple_metadata)[0]


@pytest.fixture
def items_table(simple_metadata) -> Table:
    return simple_metadata.tables["Items"]


async def test_journal_and_read_basic(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                    {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
                ],
            )

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
                insert(ct).values(
                    GroupID=1, Status="New", Count=10, Bytes=200, UpdatedAt=now
                )
            )
            # Add unfolded deltas
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 3, "BytesDelta": 50},
                ],
            )

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
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                    {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
                ],
            )

    n = await counter.fold(engine, batch_size=100)
    assert n == 2

    async with engine.connect() as conn:
        async with conn.begin():
            journal_count = (
                await conn.execute(select(func.count()).select_from(jt))
            ).scalar()
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
                insert(ct).values(
                    GroupID=1, Status="New", Count=10, Bytes=200, UpdatedAt=now
                )
            )

    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 3, "BytesDelta": 50},
                ],
            )

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
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 1, "BytesDelta": 10},
                ],
            )

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
            remaining = (
                await conn.execute(select(func.count()).select_from(jt))
            ).scalar()
    assert remaining == 7


async def test_read_exact_across_fold(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                    {"GroupID": 2, "Status": "Done", "CountDelta": 3, "BytesDelta": 60},
                ],
            )

    async with engine.connect() as conn:
        async with conn.begin():
            before = {
                (r.GroupID, r.Status): (r.Count, r.Bytes)
                for r in await counter.read(conn)
            }

    await counter.fold(engine)

    async with engine.connect() as conn:
        async with conn.begin():
            after = {
                (r.GroupID, r.Status): (r.Count, r.Bytes)
                for r in await counter.read(conn)
            }

    assert before == after


async def test_journal_rollback(engine, counter):
    jt = counter.journal_table

    try:
        async with engine.connect() as conn:
            async with conn.begin():
                await counter.journal(
                    conn,
                    [
                        {
                            "GroupID": 1,
                            "Status": "New",
                            "CountDelta": 5,
                            "BytesDelta": 100,
                        },
                    ],
                )
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
            await conn.execute(
                insert(items_table).values(
                    [
                        {"ItemID": 1, "GroupID": 1, "Status": "New", "Size": 10},
                        {"ItemID": 2, "GroupID": 1, "Status": "New", "Size": 20},
                        {"ItemID": 3, "GroupID": 1, "Status": "Done", "Size": 30},
                    ]
                )
            )
            # Seed a wrong counter row (drift)
            await conn.execute(
                insert(ct).values(
                    GroupID=1, Status="New", Count=99, Bytes=999, UpdatedAt=now
                )
            )

    counted = [
        CountedTable(
            table=items_table,
            key_exprs={
                "GroupID": items_table.c.GroupID,
                "Status": items_table.c.Status,
            },
            measure_exprs={
                "Count": func.count(),
                "Bytes": func.sum(items_table.c.Size),
            },
        )
    ]

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
            await conn.execute(
                insert(items_table).values(
                    [
                        {"ItemID": 1, "GroupID": 1, "Status": "New", "Size": 10},
                    ]
                )
            )
            await conn.execute(
                insert(ct).values(
                    GroupID=1, Status="New", Count=1, Bytes=10, UpdatedAt=now
                )
            )

    counted = [
        CountedTable(
            table=items_table,
            key_exprs={
                "GroupID": items_table.c.GroupID,
                "Status": items_table.c.Status,
            },
            measure_exprs={
                "Count": func.count(),
                "Bytes": func.sum(items_table.c.Size),
            },
        )
    ]

    n = await counter.rebuild(engine, counted)
    assert n == 0


async def test_delete_scope(engine, counter):
    ct = counter.counters_table
    jt = counter.journal_table

    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                    {"GroupID": 2, "Status": "New", "CountDelta": 3, "BytesDelta": 60},
                ],
            )

    await counter.fold(engine)

    # Add more journal rows for group 1
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
                ],
            )

    ct_deleted, jt_deleted = await counter.delete_scope(engine, {"GroupID": 1})
    assert ct_deleted == 1  # one folded counter row for group 1
    assert jt_deleted == 1  # one unfolded journal row for group 1

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
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                ],
            )

    await counter.fold(engine)

    # Transition: move 2 items from New to Done
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {
                        "GroupID": 1,
                        "Status": "New",
                        "CountDelta": -2,
                        "BytesDelta": -40,
                    },
                    {"GroupID": 1, "Status": "Done", "CountDelta": 2, "BytesDelta": 40},
                ],
            )

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result == {
        (1, "New"): (3, 60),
        (1, "Done"): (2, 40),
    }


async def test_read_with_scope(engine, counter):
    async with engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 5, "BytesDelta": 100},
                    {"GroupID": 2, "Status": "New", "CountDelta": 3, "BytesDelta": 60},
                    {"GroupID": 1, "Status": "Done", "CountDelta": 1, "BytesDelta": 10},
                ],
            )

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn, scope={"GroupID": 1})
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
            await conn.execute(
                insert(items_table).values(
                    [
                        {"ItemID": 1, "GroupID": 1, "Status": "New", "Size": 10},
                        {"ItemID": 2, "GroupID": 2, "Status": "New", "Size": 20},
                    ]
                )
            )
            # Wrong counter for group 1 only
            await conn.execute(
                insert(ct).values(
                    GroupID=1, Status="New", Count=99, Bytes=999, UpdatedAt=now
                )
            )
            await conn.execute(
                insert(ct).values(
                    GroupID=2, Status="New", Count=1, Bytes=20, UpdatedAt=now
                )
            )

    counted = [
        CountedTable(
            table=items_table,
            key_exprs={
                "GroupID": items_table.c.GroupID,
                "Status": items_table.c.Status,
            },
            measure_exprs={
                "Count": func.count(),
                "Bytes": func.sum(items_table.c.Size),
            },
        )
    ]

    n = await counter.rebuild(engine, counted, scope={"GroupID": 1})
    assert n > 0

    await counter.fold(engine)

    async with engine.connect() as conn:
        async with conn.begin():
            rows = await counter.read(conn)
    result = {(r.GroupID, r.Status): (r.Count, r.Bytes) for r in rows}
    assert result[(1, "New")] == (1, 10)
    # Group 2 untouched
    assert result[(2, "New")] == (1, 20)


# -----------------------------------------------------------------------
# (2) Error-path tests
# -----------------------------------------------------------------------


async def test_scope_rejects_non_prefix(sqlite_engine, counter):
    """Scope keys that aren't a prefix of the counter key raise ValueError."""
    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            with pytest.raises(ValueError, match="prefix"):
                await counter.read(conn, scope={"Status": "New"})


async def test_scope_rejects_unknown_key(sqlite_engine, counter):
    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            with pytest.raises(ValueError, match="prefix"):
                await counter.read(conn, scope={"Nonexistent": 1})


async def test_delete_scope_rejects_empty(sqlite_engine, counter):
    with pytest.raises(ValueError, match="non-empty"):
        await counter.delete_scope(sqlite_engine, {})


async def test_rebuild_rejects_unknown_key_exprs(sqlite_engine, counter, items_table):
    counted = [
        CountedTable(
            table=items_table,
            key_exprs={
                "GroupID": items_table.c.GroupID,
                "Nonexistent": items_table.c.Status,
            },
            measure_exprs={"Count": func.count()},
        )
    ]
    with pytest.raises(ValueError, match="must match counter key"):
        await counter.rebuild(sqlite_engine, counted)


async def test_rebuild_rejects_missing_key_exprs(sqlite_engine, counter, items_table):
    counted = [
        CountedTable(
            table=items_table,
            key_exprs={"GroupID": items_table.c.GroupID},
            measure_exprs={"Count": func.count()},
        )
    ]
    with pytest.raises(ValueError, match="must match counter key"):
        await counter.rebuild(sqlite_engine, counted)


async def test_rebuild_rejects_unknown_measure_exprs(
    sqlite_engine, counter, items_table
):
    counted = [
        CountedTable(
            table=items_table,
            key_exprs={
                "GroupID": items_table.c.GroupID,
                "Status": items_table.c.Status,
            },
            measure_exprs={
                "Count": func.count(),
                "Nonsense": func.sum(items_table.c.Size),
            },
        )
    ]
    with pytest.raises(ValueError, match="unknown measures"):
        await counter.rebuild(sqlite_engine, counted)


# -----------------------------------------------------------------------
# (5) UpdatedAt propagation through fold
# -----------------------------------------------------------------------


async def test_fold_carries_max_inserted_at(sqlite_engine, counter):
    """Fold sets UpdatedAt to MAX(InsertedAt) of the folded journal rows."""
    jt = counter.journal_table
    ct = counter.counters_table

    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [
                    {"GroupID": 1, "Status": "New", "CountDelta": 1, "BytesDelta": 10},
                ],
            )

    # Record the journal's InsertedAt
    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            journal_ts = (
                await conn.execute(select(func.max(jt.c.InsertedAt)))
            ).scalar()

    await counter.fold(sqlite_engine)

    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            counter_row = (await conn.execute(select(ct))).one()

    assert counter_row.UpdatedAt == journal_ts


async def test_fold_upsert_keeps_newer_updated_at(sqlite_engine, counter):
    """When upserting, fold keeps the newer of the two timestamps."""
    ct = counter.counters_table
    jt = counter.journal_table

    # Seed a counter row with a known timestamp
    early = datetime(2020, 1, 1, tzinfo=timezone.utc)
    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            await conn.execute(
                insert(ct).values(
                    GroupID=1, Status="New", Count=10, Bytes=100, UpdatedAt=early
                )
            )

    # Journal a delta (InsertedAt will be now, much later than 2020)
    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            await counter.journal(
                conn,
                [{"GroupID": 1, "Status": "New", "CountDelta": 1, "BytesDelta": 10}],
            )

    journal_ts = None
    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            journal_ts = (
                await conn.execute(select(func.max(jt.c.InsertedAt)))
            ).scalar()

    await counter.fold(sqlite_engine)

    async with sqlite_engine.connect() as conn:
        async with conn.begin():
            row = (await conn.execute(select(ct))).one()

    assert row.UpdatedAt == journal_ts
    assert row.UpdatedAt > early


# -----------------------------------------------------------------------
# (3)+(4) Real declaration smoke tests — BinaryUUID, native enum keys
# -----------------------------------------------------------------------


@pytest.fixture(params=["sqlite", "mysql", "mariadb"])
async def real_engine(request, mysql_url, mariadb_url):
    """Engine with the full transformation schema, parametrised over backends."""
    meta = Base.metadata
    backend = request.param

    if backend == "sqlite":
        eng = create_async_engine("sqlite+aiosqlite:///:memory:")
    elif backend == "mysql":
        eng = create_async_engine(mysql_url)
    elif backend == "mariadb":
        eng = create_async_engine(mariadb_url)
    else:
        raise ValueError(f"Unknown backend: {backend}")

    async with eng.begin() as conn:
        await conn.run_sync(meta.create_all)
        locks = counter_locks_table(meta)
        for ctr in journalled_counters(meta):
            await conn.execute(
                sa_insert(locks).prefix_with("OR IGNORE", dialect="sqlite"),
                ctr.lock_rows(),
            )

    yield eng

    if backend != "sqlite":
        async with eng.begin() as conn:
            await conn.run_sync(meta.drop_all)
    await eng.dispose()


async def test_transformation_counter_smoke(real_engine):
    """Journal, read, fold cycle against the real TRANSFORMATION_COUNTER."""
    ctr = TRANSFORMATION_COUNTER
    tid = uuid7()

    async with real_engine.connect() as conn:
        async with conn.begin():
            await ctr.journal(
                conn,
                [
                    {
                        "TransformationID": tid,
                        "Entity": "Input",
                        "Status": "Unassigned",
                        "CountDelta": 10,
                    },
                    {
                        "TransformationID": tid,
                        "Entity": "Parcel",
                        "Status": "Assigned",
                        "CountDelta": 3,
                    },
                ],
            )

    async with real_engine.connect() as conn:
        async with conn.begin():
            rows = await ctr.read(conn, scope={"TransformationID": tid})
    assert len(rows) == 2
    result = {(r.Entity, r.Status): r.Count for r in rows}
    assert result[("Input", "Unassigned")] == 10
    assert result[("Parcel", "Assigned")] == 3

    n = await ctr.fold(real_engine)
    assert n == 2

    async with real_engine.connect() as conn:
        async with conn.begin():
            rows = await ctr.read(conn, scope={"TransformationID": tid})
    result2 = {(r.Entity, r.Status): r.Count for r in rows}
    assert result2 == result


async def test_data_parcels_counter_smoke(real_engine):
    """Journal, read, fold cycle against the real DATA_PARCELS_COUNTER."""
    ctr = DATA_PARCELS_COUNTER
    tid = uuid7()

    async with real_engine.connect() as conn:
        async with conn.begin():
            await ctr.journal(
                conn,
                [
                    {
                        "TransformationID": tid,
                        "Status": "Unassigned",
                        "StorageElement": "SE1",
                        "LFNCountAddedDelta": 5,
                        "LFNSizeAddedDelta": 1000,
                        "LFNCountFreedDelta": 0,
                        "LFNSizeFreedDelta": 0,
                    },
                ],
            )

    async with real_engine.connect() as conn:
        async with conn.begin():
            rows = await ctr.read(conn, scope={"TransformationID": tid})
    assert len(rows) == 1
    assert rows[0].LFNCountAdded == 5
    assert rows[0].LFNSizeAdded == 1000

    n = await ctr.fold(real_engine)
    assert n == 1

    async with real_engine.connect() as conn:
        async with conn.begin():
            rows = await ctr.read(conn, scope={"TransformationID": tid})
    assert rows[0].LFNCountAdded == 5
