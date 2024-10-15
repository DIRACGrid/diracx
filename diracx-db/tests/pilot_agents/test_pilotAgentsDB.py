from __future__ import annotations

import pytest

from diracx.db.sql.pilot_agents.db import PilotAgentsDB


@pytest.fixture
async def pilot_agents_db(tmp_path) -> PilotAgentsDB:
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
