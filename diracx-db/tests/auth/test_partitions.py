"""Tests for the RefreshTokens partition-maintenance logic.

The pure planner (``plan_partition_maintenance``) and the name/boundary helpers
are dialect-independent and exercised directly here. The MySQL-only executor
(``maintain_refresh_token_partitions``) cannot be run against the in-memory
SQLite test database, so we only assert that it refuses to run on SQLite.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from dateutil.relativedelta import relativedelta

from diracx.db.sql.auth.db import (
    AuthDB,
    _partition_boundary,
    _partition_name,
    plan_partition_maintenance,
)
from diracx.db.sql.utils import uuid7_from_datetime


def m(year: int, month: int) -> datetime:
    """Month-start datetime helper."""
    return datetime(year, month, 1, tzinfo=UTC)


@pytest.fixture
async def auth_db(tmp_path):
    auth_db = AuthDB("sqlite+aiosqlite:///:memory:")
    async with auth_db.engine_context():
        async with auth_db.engine.begin() as conn:
            await conn.run_sync(auth_db.metadata.create_all)
        yield auth_db


# --- helpers ---------------------------------------------------------------


def test_partition_name():
    assert _partition_name(m(2026, 3)) == "p_2026_3"
    assert _partition_name(m(2026, 12)) == "p_2026_12"


def test_partition_boundary_matches_uuid7():
    dt = m(2026, 4)
    boundary = _partition_boundary(dt)
    # The boundary is the dash-stripped lowest UUIDv7 for the timestamp.
    assert boundary == str(uuid7_from_datetime(dt, randomize=False)).replace("-", "")
    assert len(boundary) == 32  # 32 hex chars, no dashes


def test_partition_boundary_is_monotonic():
    # The executor relies on lexical ordering of the JTI string boundaries.
    assert _partition_boundary(m(2026, 1)) < _partition_boundary(m(2026, 2))
    assert _partition_boundary(m(2026, 12)) < _partition_boundary(m(2027, 1))


# --- planner: drop ---------------------------------------------------------


def test_plan_drops_only_fully_expired_partitions():
    now = datetime(2026, 6, 15, tzinfo=UTC)
    existing = [m(2026, month) for month in range(1, 9)]  # Jan..Aug 2026

    to_drop, _ = plan_partition_maintenance(
        existing, now=now, retention_months=1, months_ahead=0
    )

    # A partition for month X has upper bound X+1mo; drop when that is older than
    # now - 1 month (2026-05-15). Jan..Apr have bounds Feb1..May1 (all <= May15).
    assert to_drop == [m(2026, 1), m(2026, 2), m(2026, 3), m(2026, 4)]


def test_plan_drop_boundary_is_inclusive():
    # Upper bound exactly equal to the horizon must be dropped (<=).
    now = m(2026, 6)
    # now - 1 month == 2026-05-01
    existing = [m(2026, 4), m(2026, 5)]  # bounds: May1, Jun1

    to_drop, _ = plan_partition_maintenance(
        existing, now=now, retention_months=1, months_ahead=0
    )
    assert to_drop == [m(2026, 4)]  # May1 <= May1 drops April; June kept


def test_plan_keeps_last_six_months_by_default():
    # The deployment policy: keep the last 6 months worth of refresh tokens.
    now = datetime(2026, 7, 15, tzinfo=UTC)
    existing = [m(2025, month) for month in range(6, 13)] + [
        m(2026, month) for month in range(1, 8)
    ]  # 2025-06 .. 2026-07

    to_drop, _ = plan_partition_maintenance(
        existing, now=now, retention_months=6, months_ahead=0
    )

    # Horizon is 2026-01-15: nothing from the last 6 months is dropped.
    assert all(d < m(2026, 1) for d in to_drop)
    assert m(2025, 12) in to_drop
    assert m(2026, 1) not in to_drop
    assert max(to_drop) == m(2025, 12)


def test_plan_keeps_everything_when_retention_is_huge():
    now = datetime(2026, 6, 15, tzinfo=UTC)
    existing = [m(2026, month) for month in range(1, 9)]
    to_drop, _ = plan_partition_maintenance(
        existing, now=now, retention_months=120, months_ahead=0
    )
    assert to_drop == []


# --- planner: add ----------------------------------------------------------


def test_plan_adds_months_up_to_horizon():
    now = datetime(2026, 6, 15, tzinfo=UTC)
    existing = [m(2026, 7)]  # highest existing partition is July
    _, to_add = plan_partition_maintenance(
        existing, now=now, retention_months=6, months_ahead=3
    )
    # target_last = month_start(now) + 3 = 2026-09; append above July.
    assert to_add == [m(2026, 8), m(2026, 9)]


def test_plan_adds_nothing_when_buffer_already_covered():
    now = datetime(2026, 6, 15, tzinfo=UTC)
    existing = [m(2026, month) for month in range(6, 10)]  # Jun..Sep
    _, to_add = plan_partition_maintenance(
        existing, now=now, retention_months=6, months_ahead=3
    )
    assert to_add == []  # highest existing (Sep) already == now+3mo


def test_plan_crosses_year_boundary():
    now = datetime(2026, 11, 15, tzinfo=UTC)
    existing = [m(2026, 11)]
    _, to_add = plan_partition_maintenance(
        existing, now=now, retention_months=6, months_ahead=3
    )
    assert to_add == [m(2026, 12), m(2027, 1), m(2027, 2)]


def test_plan_empty_existing_seeds_from_current_month():
    now = datetime(2026, 6, 15, tzinfo=UTC)
    _, to_add = plan_partition_maintenance(
        [], now=now, retention_months=6, months_ahead=2
    )
    # No partitions yet: seed current month + buffer.
    assert to_add == [m(2026, 6), m(2026, 7), m(2026, 8)]


def test_plan_combined_drop_and_add():
    now = datetime(2026, 6, 15, tzinfo=UTC)
    existing = [m(2026, month) for month in range(1, 8)]  # Jan..Jul
    to_drop, to_add = plan_partition_maintenance(
        existing, now=now, retention_months=1, months_ahead=2
    )
    assert to_drop == [m(2026, 1), m(2026, 2), m(2026, 3), m(2026, 4)]
    assert to_add == [m(2026, 8)]  # target_last = 2026-08, append above July


def test_plan_added_months_are_contiguous_and_increasing():
    now = datetime(2026, 6, 15, tzinfo=UTC)
    existing = [m(2026, 6)]
    _, to_add = plan_partition_maintenance(
        existing, now=now, retention_months=6, months_ahead=12
    )
    # Each added month is exactly one month after the previous.
    for previous, current in zip(to_add, to_add[1:]):
        assert current == previous + relativedelta(months=1)
    assert to_add[0] == m(2026, 7)
    assert to_add[-1] == m(2027, 6)  # now + 12 months


# --- executor: dialect guard ----------------------------------------------


async def test_maintain_partitions_requires_mysql(auth_db: AuthDB):
    async with auth_db as auth_db:
        with pytest.raises(NotImplementedError, match="MySQL"):
            await auth_db.maintain_refresh_token_partitions(retention_months=6)
