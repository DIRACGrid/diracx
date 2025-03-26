from __future__ import annotations

import pytest
from sqlalchemy.exc import NoResultFound

from diracx.core.exceptions import AuthorizationError
from diracx.db.sql.pilot_agents.db import PilotAgentsDB


@pytest.fixture
async def pilot_agents_db(tmp_path):
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


async def test_insert_and_select(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        # Add a pilot reference
        refs = [f"ref_{i}" for i in range(10)]
        stamps = [f"stamp_{i}" for i in range(10)]
        stamp_dict = dict(zip(refs, stamps))

        await pilot_agents_db.add_pilot_references(
            refs, "test_vo", grid_type="DIRAC", pilot_stamps=stamp_dict
        )

        await pilot_agents_db.add_pilot_references(
            refs, "test_vo", grid_type="DIRAC", pilot_stamps=None
        )


async def test_insert_and_select_single(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        new_pilot_id = await pilot_agents_db.register_new_pilot(vo="pilot-vo")

        res = await pilot_agents_db.get_pilot_by_id(new_pilot_id)

        with pytest.raises(NoResultFound):
            await pilot_agents_db.get_pilot_by_id(10)

        # Set values
        assert res["PilotID"] == new_pilot_id
        assert res["VO"] == "pilot-vo"

        # Default values
        assert res["PilotStamp"] == ""
        assert res["BenchMark"] == 0.0
        assert res["Status"] == "Unknown"


async def test_create_pilot_and_verify_secret(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        new_pilot_id = await pilot_agents_db.register_new_pilot(vo="pilot-vo")

        # Add creds
        secret = await pilot_agents_db.add_pilot_credentials(new_pilot_id)

        assert secret is not None

        await pilot_agents_db.verify_pilot_secret(
            pilot_id=new_pilot_id, pilot_secret=secret
        )

        with pytest.raises(AuthorizationError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_id=new_pilot_id, pilot_secret="I love stawberries :)"
            )

            await pilot_agents_db.verify_pilot_secret(
                pilot_id=63000, pilot_secret=secret
            )
