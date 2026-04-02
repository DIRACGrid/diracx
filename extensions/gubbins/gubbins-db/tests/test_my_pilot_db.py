from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gubbins.db.sql.my_pilot_db.db import MyPilotDB
from gubbins.db.sql.my_pilot_db.schema import MyPilotStatus

if TYPE_CHECKING:
    from typing import AsyncGenerator


@pytest.fixture
async def my_pilot_db(tmp_path) -> AsyncGenerator[MyPilotDB, None]:
    my_pilot_db = MyPilotDB("sqlite+aiosqlite:///:memory:")
    async with my_pilot_db.engine_context():
        async with my_pilot_db.engine.begin() as conn:
            await conn.run_sync(my_pilot_db.metadata.create_all)
        yield my_pilot_db


async def test_add_and_get_ces(my_pilot_db: MyPilotDB):
    async with my_pilot_db as db:
        await db.add_ce("reliable-ce.example.org", capacity=5, success_rate=1.0)
        await db.add_ce("flaky-ce.example.org", capacity=3, success_rate=0.3)

    async with my_pilot_db as db:
        ces = await db.get_available_ces()
        assert len(ces) == 2
        names = {ce["name"] for ce in ces}
        assert names == {"reliable-ce.example.org", "flaky-ce.example.org"}


async def test_disabled_ce_not_available(my_pilot_db: MyPilotDB):
    async with my_pilot_db as db:
        await db.add_ce("disabled-ce", capacity=5, success_rate=1.0, enabled=False)

    async with my_pilot_db as db:
        ces = await db.get_available_ces()
        assert len(ces) == 0


async def test_submit_pilot(my_pilot_db: MyPilotDB):
    async with my_pilot_db as db:
        await db.add_ce("test-ce", capacity=5, success_rate=1.0)

    async with my_pilot_db as db:
        pilot_id = await db.submit_pilot("test-ce")
        assert pilot_id is not None

    async with my_pilot_db as db:
        pilots = await db.get_pilots_by_status(MyPilotStatus.SUBMITTED)
        assert len(pilots) == 1
        assert pilots[0]["ce_name"] == "test-ce"


async def test_update_pilot_status(my_pilot_db: MyPilotDB):
    async with my_pilot_db as db:
        await db.add_ce("test-ce", capacity=5, success_rate=1.0)

    async with my_pilot_db as db:
        pilot_id = await db.submit_pilot("test-ce")

    async with my_pilot_db as db:
        await db.update_pilot_status(pilot_id, MyPilotStatus.RUNNING)

    async with my_pilot_db as db:
        submitted = await db.get_pilots_by_status(MyPilotStatus.SUBMITTED)
        running = await db.get_pilots_by_status(MyPilotStatus.RUNNING)
        assert len(submitted) == 0
        assert len(running) == 1


async def test_capacity_tracking(my_pilot_db: MyPilotDB):
    async with my_pilot_db as db:
        await db.add_ce("small-ce", capacity=2, success_rate=1.0)

    # Submit 2 pilots to fill capacity
    async with my_pilot_db as db:
        await db.submit_pilot("small-ce")
        await db.submit_pilot("small-ce")

    # CE should no longer be available
    async with my_pilot_db as db:
        ces = await db.get_available_ces()
        assert len(ces) == 0

    # Complete one pilot — frees a slot
    async with my_pilot_db as db:
        pilots = await db.get_pilots_by_status(MyPilotStatus.SUBMITTED)
        await db.update_pilot_status(pilots[0]["pilot_id"], MyPilotStatus.DONE)

    async with my_pilot_db as db:
        ces = await db.get_available_ces()
        assert len(ces) == 1
        assert ces[0]["available_slots"] == 1


async def test_get_ce_success_rate(my_pilot_db: MyPilotDB):
    async with my_pilot_db as db:
        await db.add_ce("test-ce", capacity=5, success_rate=0.75)

    async with my_pilot_db as db:
        rate = await db.get_ce_success_rate("test-ce")
        assert rate == 0.75


async def test_pilot_summary(my_pilot_db: MyPilotDB):
    async with my_pilot_db as db:
        await db.add_ce("test-ce", capacity=10, success_rate=1.0)

    async with my_pilot_db as db:
        await db.submit_pilot("test-ce")
        await db.submit_pilot("test-ce")
        pilot_id = await db.submit_pilot("test-ce")

    async with my_pilot_db as db:
        await db.update_pilot_status(pilot_id, MyPilotStatus.RUNNING)

    async with my_pilot_db as db:
        summary = await db.get_pilot_summary()
        assert summary[MyPilotStatus.SUBMITTED] == 2
        assert summary[MyPilotStatus.RUNNING] == 1
