from __future__ import annotations

from datetime import timedelta
from random import shuffle
from time import sleep

import pytest

from diracx.core.exceptions import (
    BadPilotVOError,
    CredentialsNotFoundError,
    OverusedSecretError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
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
        # Add pilots
        refs = [f"ref_{i}" for i in range(10)]
        stamps = [f"stamp_{i}" for i in range(10)]
        pilot_references = dict(zip(stamps, refs))

        await pilot_agents_db.add_pilots_bulk(
            stamps, "test_vo", grid_type="DIRAC", pilot_references=pilot_references
        )

        await pilot_agents_db.add_pilots_bulk(
            stamps, "test_vo", grid_type="DIRAC", pilot_references=None
        )


async def test_insert_and_select_single(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        pilot_stamp = "pilot-reference-test"
        await pilot_agents_db.add_pilots_bulk(
            vo="lhcb",
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        res = await pilot_agents_db.get_pilots_by_stamp_bulk([pilot_stamp])

        assert len(res) == 1

        pilot = res[0]

        with pytest.raises(PilotNotFoundError):
            await pilot_agents_db.get_pilots_by_stamp_bulk(["I am a fake stamp"])

        # Set values
        assert pilot["VO"] == "lhcb"
        assert pilot["PilotStamp"] == pilot_stamp
        assert pilot["GridType"] == "grid-type"


async def test_create_pilot_and_verify_secret(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(100)]
        stamps = [f"stamp_{i}" for i in range(100)]
        pilot_references = dict(zip(stamps, refs))

        vo = "test_vo"

        await pilot_agents_db.add_pilots_bulk(
            stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
        )

        pilots = await pilot_agents_db.get_pilots_by_stamp_bulk(stamps)

        pilot_ids = [pilot["PilotID"] for pilot in pilots]

        secrets = [f"AW0nd3rfulS3cr3t_{pilot_id}" for pilot_id in pilot_ids]
        pilot_hashed_secrets = [hash(secret) for secret in secrets]

        # Add creds
        added_secrets = await pilot_agents_db.add_pilots_credentials_bulk(
            pilot_stamps=stamps,
            pilot_hashed_secrets=pilot_hashed_secrets,
            pilot_secret_use_count_max=1,
            vo=vo,
        )

        assert len(added_secrets) == len(pilots)

        # Extract dates
        creation_dates = [secret["SecretCreationDate"] for secret in added_secrets]
        pilot_secret_ids = [secret["SecretID"] for secret in added_secrets]

        expiration_dates = [
            creation_date + timedelta(seconds=10) for creation_date in creation_dates
        ]

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_ids=pilot_secret_ids, pilot_secret_expiration_dates=expiration_dates
        )

        pairs = list(zip(stamps, secrets))
        # Shuffle it to prove that credentials are well associated
        shuffle(pairs)

        for stamp, secret in pairs:
            await pilot_agents_db.verify_pilot_secret(
                pilot_stamp=stamp, pilot_hashed_secret=hash(secret)
            )

        with pytest.raises(SecretNotFoundError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_stamp=stamps[0],
                pilot_hashed_secret=hash("I love stawberries :)"),
            )

        with pytest.raises(CredentialsNotFoundError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_stamp="I am a spider",
                pilot_hashed_secret=secrets[0],
            )


async def test_create_pilot_and_verify_secret_with_delay(
    pilot_agents_db: PilotAgentsDB,
):

    async with pilot_agents_db as pilot_agents_db:
        pilot_stamp = "pilot-stamp"
        vo = "lhcb"
        # Register a pilot
        await pilot_agents_db.add_pilots_bulk(
            vo=vo,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        secret = "AW0nd3rfulS3cr3t"
        pilot_hashed_secret = hash(secret)

        # Add creds
        secrets_added = await pilot_agents_db.add_pilots_credentials_bulk(
            pilot_stamps=[pilot_stamp],
            pilot_hashed_secrets=[pilot_hashed_secret],
            pilot_secret_use_count_max=10,
            vo=vo,
        )

        assert len(secrets_added) == 1
        secret = secrets_added[0]

        expiration_date = secret["SecretCreationDate"] + timedelta(seconds=1)

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_ids=[secret["SecretID"]],
            pilot_secret_expiration_dates=[expiration_date],
        )

        assert secret is not None

        # So that the secret expires
        sleep(1)

        with pytest.raises(SecretHasExpiredError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_stamp=pilot_stamp,
                pilot_hashed_secret=pilot_hashed_secret,
            )


async def test_create_pilot_and_verify_secret_too_much_secret_use(
    pilot_agents_db: PilotAgentsDB,
):

    async with pilot_agents_db as pilot_agents_db:
        pilot_stamp = "pilot-stamp"
        vo = "lhcb"
        # Register a pilot
        await pilot_agents_db.add_pilots_bulk(
            vo=vo,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        secret = "AW0nd3rfulS3cr3t"
        pilot_hashed_secret = hash(secret)

        # Add creds
        secrets_added = await pilot_agents_db.add_pilots_credentials_bulk(
            pilot_stamps=[pilot_stamp],
            pilot_hashed_secrets=[pilot_hashed_secret],
            pilot_secret_use_count_max=1,
            vo=vo,
        )

        assert len(secrets_added) == 1
        secret = secrets_added[0]

        expiration_date = secret["SecretCreationDate"] + timedelta(seconds=10)

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_ids=[secret["SecretID"]],
            pilot_secret_expiration_dates=[expiration_date],
        )

        # First login, should work
        await pilot_agents_db.verify_pilot_secret(
            pilot_stamp=pilot_stamp,
            pilot_hashed_secret=pilot_hashed_secret,
        )

        # Second login, should not work because maxed out at 1 try
        with pytest.raises(OverusedSecretError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_stamp=pilot_stamp,
                pilot_hashed_secret=pilot_hashed_secret,
            )


async def test_create_pilot_and_login_with_wrong_vo(
    pilot_agents_db: PilotAgentsDB,
):
    async with pilot_agents_db as pilot_agents_db:

        #  ----------------- Part 1 : Create a pilot with vo_1 -----------------
        pilot_stamp_1 = "pilot-stamp"
        vo_1 = "lhcb"
        # Register a pilot
        await pilot_agents_db.add_pilots_bulk(
            vo=vo_1,
            pilot_stamps=[pilot_stamp_1],
            grid_type="grid-type",
        )

        secret_text = "AW0nd3rfulS3cr3t"
        pilot_hashed_secret = hash(secret_text)

        # Add creds
        secrets_added = await pilot_agents_db.add_pilots_credentials_bulk(
            pilot_stamps=[pilot_stamp_1],
            pilot_hashed_secrets=[pilot_hashed_secret],
            pilot_secret_use_count_max=2,  # Important later
            vo=vo_1,
        )

        assert len(secrets_added) == 1
        secret = secrets_added[0]

        expiration_date = secret["SecretCreationDate"] + timedelta(seconds=10)

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_ids=[secret["SecretID"]],
            pilot_secret_expiration_dates=[expiration_date],
        )

        # First login, should work
        await pilot_agents_db.verify_pilot_secret(
            pilot_stamp=pilot_stamp_1,
            pilot_hashed_secret=pilot_hashed_secret,
        )

        #  ----------------- Part 1 : Create a pilot with vo_1 -----------------
        pilot_stamp_2 = "pilot-stamp-2"
        vo_2 = "lhcb_2"

        # Register a second pilot
        await pilot_agents_db.add_pilots_bulk(
            vo=vo_2,
            pilot_stamps=[pilot_stamp_2],
            grid_type="grid-type",
        )

        # Add creds with a bad vo
        with pytest.raises(BadPilotVOError) as exc_info:
            secrets_added = await pilot_agents_db.associate_pilots_with_secrets_bulk(
                [{"PilotSecretID": secret["SecretID"], "PilotStamp": pilot_stamp_2}]
            )

        assert "Bad VO" in str(exc_info.value)
