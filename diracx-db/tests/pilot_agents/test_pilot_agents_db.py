from __future__ import annotations

from datetime import timedelta
from random import shuffle
from time import sleep

import pytest

from diracx.core.exceptions import AuthorizationError, PilotNotFoundError
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
            pilot_refs=[pilot_reference],
            grid_type="grid-type",
        )

        res = await pilot_agents_db.get_pilots_by_references_bulk([pilot_reference])

        assert len(res) == 1

        pilot = res[0]

        with pytest.raises(PilotNotFoundError):
            await pilot_agents_db.get_pilots_by_references_bulk(["I am a fake ref"])

        # Set values
        assert pilot["VO"] == "lhcb"
        assert pilot["PilotJobReference"] == pilot_reference
        assert pilot["GridType"] == "grid-type"


async def test_create_pilot_and_verify_secret(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        refs = [f"ref_{i}" for i in range(100)]
        stamps = [f"stamp_{i}" for i in range(100)]
        stamp_dict = dict(zip(refs, stamps))

        # Verify that they don't exist
        with pytest.raises(PilotNotFoundError):
            await pilot_agents_db.get_pilots_by_references_bulk(refs)

        # Register a pilot
        await pilot_agents_db.add_pilot_references(
            vo="lhcb", pilot_refs=refs, grid_type="grid-type", pilot_stamps=stamp_dict
        )

        pilots = await pilot_agents_db.get_pilots_by_references_bulk(refs)

        assert len(pilots) == len(refs)

        pilot_ids = [pilot["PilotID"] for pilot in pilots]

        secrets = [f"AW0nd3rfulS3cr3t_{pilot_id}" for pilot_id in pilot_ids]
        pilot_hashed_secrets = [hash(secret) for secret in secrets]

        # Add creds
        date_added = await pilot_agents_db.add_pilots_credentials(
            pilot_ids=pilot_ids, pilot_hashed_secrets=pilot_hashed_secrets
        )

        assert len(date_added) == len(pilots)

        expiration_dates = [date + timedelta(seconds=10) for date in date_added]

        await pilot_agents_db.set_pilot_credentials_expiration(
            pilot_ids=pilot_ids, pilot_secret_expiration_dates=expiration_dates
        )

        assert all(secret is not None for secret in secrets)

        pairs = list(zip(refs, secrets))
        # Shuffle it to prove that credentials are well associated
        shuffle(pairs)

        for pilot_reference, secret in pairs:
            await pilot_agents_db.verify_pilot_secret(
                pilot_job_reference=pilot_reference, pilot_hashed_secret=hash(secret)
            )

        with pytest.raises(AuthorizationError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_job_reference=refs[0],
                pilot_hashed_secret=hash("I love stawberries :)"),
            )

            await pilot_agents_db.verify_pilot_secret(
                pilot_job_reference="I am a spider",
                pilot_hashed_secret=secrets[0],
            )


async def test_create_pilot_and_verify_secret_with_delay(
    pilot_agents_db: PilotAgentsDB,
):

    async with pilot_agents_db as pilot_agents_db:
        pilot_reference = "pilot-reference-test"
        # Register a pilot
        await pilot_agents_db.add_pilot_references(
            vo="lhcb",
            pilot_refs=[pilot_reference],
            grid_type="grid-type",
        )

        pilots = await pilot_agents_db.get_pilots_by_references_bulk([pilot_reference])

        assert len(pilots) == 1

        pilot = pilots[0]

        pilot_id = pilot["PilotID"]

        secret = "AW0nd3rfulS3cr3t"
        pilot_hashed_secret = hash(secret)

        # Add creds
        date_added = await pilot_agents_db.add_pilots_credentials(
            pilot_ids=[pilot_id], pilot_hashed_secrets=[pilot_hashed_secret]
        )

        assert len(date_added) == 1

        expiration_date = date_added[0] + timedelta(seconds=1)

        await pilot_agents_db.set_pilot_credentials_expiration(
            pilot_ids=[pilot_id], pilot_secret_expiration_dates=[expiration_date]
        )

        assert secret is not None

        # So that the secret expires
        sleep(3)

        with pytest.raises(AuthorizationError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_job_reference=pilot_reference,
                pilot_hashed_secret=pilot_hashed_secret,
            )
