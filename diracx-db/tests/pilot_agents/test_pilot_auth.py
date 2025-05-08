from __future__ import annotations

from datetime import timedelta
from random import shuffle
from time import sleep

import pytest

from diracx.core.exceptions import (
    BadPilotVOError,
    CredentialsNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.db.sql.pilot_agents.db import PilotAgentsDB
from diracx.db.sql.utils.functions import hash

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
async def pilot_agents_db(tmp_path):
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


@pytest.fixture
async def add_stamps(pilot_agents_db):
    async with pilot_agents_db as pilot_agents_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(N)]
        stamps = [f"stamp_{i}" for i in range(N)]
        pilot_references = dict(zip(stamps, refs))

        vo = MAIN_VO

        await pilot_agents_db.add_pilots_bulk(
            stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
        )

        pilots = await pilot_agents_db.get_pilots_by_stamp_bulk(stamps)

        return pilots


@pytest.fixture
async def add_secrets_and_time(pilot_agents_db, add_stamps, secret_duration_sec):

    async with pilot_agents_db as pilot_agents_db:
        # Retrieve the stamps from the add_stamps fixture
        stamps = [pilot["PilotStamp"] for pilot in add_stamps]

        secrets = [f"AW0nd3rfulS3cr3t_{str(i)}" for i in range(len(stamps))]
        hashed_secrets = [hash(secret) for secret in secrets]

        # Add creds
        await pilot_agents_db.insert_unique_secrets_bulk(
            hashed_secrets=hashed_secrets, vo=MAIN_VO
        )

        # Associate with pilot
        secrets_obj = await pilot_agents_db.get_secrets_by_hashed_secrets_bulk(
            hashed_secrets
        )

        assert len(secrets_obj) == len(hashed_secrets) == len(stamps)

        # Associate pilot with its secret
        pilot_to_secret_id_mapping_values = [
            {
                "PilotSecretID": secret["SecretID"],
                "PilotStamp": stamp,
            }
            for secret, stamp in zip(secrets_obj, stamps)
        ]
        await pilot_agents_db.associate_pilots_with_secrets_bulk(
            pilot_to_secret_id_mapping_values
        )

        expiration_date = [
            secret_obj["SecretCreationDate"] + timedelta(seconds=secret_duration_sec)
            for secret_obj in secrets_obj
        ]

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_ids=[secret_obj["SecretID"] for secret_obj in secrets_obj],
            pilot_secret_expiration_dates=expiration_date,
        )

        # Return both non-hashed secrets and stamps
        return {"stamps": stamps, "secrets": secrets}


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret(
    pilot_agents_db: PilotAgentsDB, add_secrets_and_time
):

    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pairs = list(zip(stamps, secrets))
    # Shuffle it to prove that credentials are well associated
    shuffle(pairs)

    async with pilot_agents_db as pilot_agents_db:
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
                pilot_hashed_secret=hash(secrets[0]),
            )


@pytest.mark.parametrize("secret_duration_sec", [1])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret_with_delay(
    pilot_agents_db: PilotAgentsDB, add_secrets_and_time
):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    # So that the secret expires
    sleep(1)

    async with pilot_agents_db as pilot_agents_db:
        with pytest.raises(SecretHasExpiredError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_stamp=stamps[0],
                pilot_hashed_secret=hash(secrets[0]),
            )


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret_too_much_secret_use(
    pilot_agents_db: PilotAgentsDB, add_secrets_and_time
):

    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    async with pilot_agents_db as pilot_agents_db:
        # First login, should work
        await pilot_agents_db.verify_pilot_secret(
            pilot_stamp=stamps[0],
            pilot_hashed_secret=hash(secrets[0]),
        )

        # Second login, should not work because maxed out at 1 try
        # If the foreign key works, we should have "SecretNotFoundError"
        with pytest.raises(SecretNotFoundError):
            await pilot_agents_db.verify_pilot_secret(
                pilot_stamp=stamps[0],
                pilot_hashed_secret=hash(secrets[0]),
            )


@pytest.mark.asyncio
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
        await pilot_agents_db.insert_unique_secrets_bulk(
            hashed_secrets=[pilot_hashed_secret], vo=vo_1, secret_global_use_count_max=2
        )

        # Associate with pilot
        # Get the secret ids to later associate them with pilots
        secrets = await pilot_agents_db.get_secrets_by_hashed_secrets_bulk(
            [pilot_hashed_secret]
        )

        assert len(secrets) == 1
        secret_obj = secrets[0]
        # Associate pilot with its secret
        pilot_to_secret_id_mapping_values = [
            {
                "PilotSecretID": secret_obj["SecretID"],
                "PilotStamp": pilot_stamp_1,
            }
        ]
        await pilot_agents_db.associate_pilots_with_secrets_bulk(
            pilot_to_secret_id_mapping_values
        )

        expiration_date = secret_obj["SecretCreationDate"] + timedelta(seconds=10)

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_ids=[secret_obj["SecretID"]],
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
            await pilot_agents_db.associate_pilots_with_secrets_bulk(
                [{"PilotSecretID": secret_obj["SecretID"], "PilotStamp": pilot_stamp_2}]
            )

        assert "Bad VO" in str(exc_info.value)
