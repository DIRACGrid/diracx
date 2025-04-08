from __future__ import annotations

import pytest
from sqlalchemy.exc import NoResultFound

from diracx.core.exceptions import AuthorizationError
from diracx.db.sql.pilot_agents.db import PilotAgentsDB
from diracx.db.sql.utils.functions import hash


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
        pilot_reference = "pilot-reference-test"
        await pilot_agents_db.add_pilot_references(
            vo="lhcb",
            pilot_ref=[pilot_reference],
            grid_type="grid-type",
        )

        res = await pilot_agents_db.get_pilot_by_reference(pilot_ref=pilot_reference)

        with pytest.raises(NoResultFound):
            await pilot_agents_db.get_pilot_by_reference("I am a fake ref")

        # Set values
        assert res["VO"] == "lhcb"
        assert res["PilotJobReference"] == pilot_reference
        assert res["GridType"] == "grid-type"


async def test_create_pilot_and_verify_secret(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        pilot_reference = "pilot-reference-test"
        # Register a pilot
        await pilot_agents_db.add_pilot_references(
            vo="lhcb",
            pilot_ref=[pilot_reference],
            grid_type="grid-type",
        )

        pilot = await pilot_agents_db.get_pilot_by_reference(pilot_reference)

        pilot_id = pilot["PilotID"]

        secret = "AW0nd3rfulS3cr3t"
        pilot_hashed_secret = hash(secret)

        # Add creds
        await pilot_agents_db.add_pilot_credentials(
            pilot_id=pilot_id, pilot_hashed_secret=pilot_hashed_secret
        )

        assert secret is not None

        await pilot_agents_db.verify_pilot_secret(
            pilot_job_reference=pilot_reference, pilot_hashed_secret=pilot_hashed_secret
        )

        with pytest.raises(AuthorizationError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_job_reference=pilot_reference,
                pilot_hashed_secret=hash("I love stawberries :)"),
            )

            await pilot_agents_db.verify_pilot_secret(
                pilot_job_reference="I am a spider",
                pilot_hashed_secret=pilot_hashed_secret,
            )
