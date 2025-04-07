from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import shuffle
from typing import AsyncGenerator, Generator

import freezegun
import pytest
import sqlalchemy

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.db.exceptions import DBInBadStateError
from diracx.db.sql.pilot_agents.db import PilotAgentsDB
from diracx.db.sql.utils.functions import hash
from diracx.testing.time import mock_sqlite_time

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
async def pilot_agents_db() -> AsyncGenerator[PilotAgentsDB, None]:
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        sqlalchemy.event.listen(
            agents_db.engine.sync_engine, "connect", mock_sqlite_time
        )
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


@pytest.fixture()
def frozen_time() -> Generator[freezegun.FreezeGun, None]:
    with freezegun.freeze_time("2012-01-14") as ft:
        yield ft


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
async def add_secrets_and_time(
    pilot_agents_db, add_stamps, secret_duration_sec, frozen_time: freezegun.FreezeGun
):

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
                "b_PilotSecretID": secret["SecretID"],
                "b_PilotStamp": stamp,
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


async def verify_pilot_secret(
    pilot_stamp: str, pilot_db: PilotAgentsDB, hashed_secret: str
) -> None:

    # 1. Get the pilot
    pilots = await pilot_db.get_pilots_by_stamp_bulk([pilot_stamp])
    pilot = dict(pilots[0])  # Semantic, assured by fetch_records_bulk_or_raises
    real_secret_id = pilot["PilotSecretID"]

    # 2. Get the secret itself
    given_secrets = await pilot_db.get_secrets_by_hashed_secrets_bulk([hashed_secret])
    given_secret = given_secrets[0]
    given_secret_id = given_secret[
        "SecretID"
    ]  # Semantic, assured by fetch_records_bulk_or_raises

    # 3. Compare the secret_id
    # If SecretID is NULL
    if not real_secret_id or given_secret_id != real_secret_id:
        raise BadPilotCredentialsError(
            data={
                "pilot_stamp": pilot_stamp,
                "pilot_hashed_secret": hashed_secret,
                "real_secret_id": str(real_secret_id),
                "given_secret_id": str(given_secret_id),
            }
        )

    # 4. Check if the secret is expired
    now = datetime.now(tz=timezone.utc)
    # Convert the timezone, TODO: Change with #454: https://github.com/DIRACGrid/diracx/pull/454
    expiration = given_secret["SecretExpirationDate"].replace(tzinfo=timezone.utc)
    if expiration < now:

        try:
            await pilot_db.delete_secrets_bulk([real_secret_id])
        except SecretNotFoundError as e:
            raise DBInBadStateError(
                detail="This should not happen. Pilot should have a secret, but not found."
            ) from e

        raise SecretHasExpiredError(
            data={
                "pilot_hashed_secret": hashed_secret,
                "now": str(now),
                "expiration_date": given_secret["SecretExpirationDate"],
            }
        )

    # 5. Now the pilot is authorized, change when the pilot used the secret.
    try:
        await pilot_db.update_pilot_secret_use_time(
            pilot_stamp=pilot_stamp,
        )
    except Exception as e:  # Generic, to catch it.
        # Should NOT happen
        # Wrapped in a try/catch to still catch in case of an error in the counters
        # Caught and raised here to avoid raising a 4XX error
        raise DBInBadStateError(
            detail="This should not happen. Pilot has credentials, but has a corrupted secret."
        ) from e

    # 6. Delete the secret if its count attained the secret_global_use_count_max
    if given_secret["SecretGlobalUseCountMax"]:
        if (
            given_secret["SecretGlobalUseCount"] + 1
            == given_secret["SecretGlobalUseCountMax"]
        ):
            try:
                await pilot_db.delete_secrets_bulk([given_secret_id])
            except SecretNotFoundError as e:
                # Should NOT happen
                raise DBInBadStateError(
                    detail="This should not happen. Pilot has credentials, but has corrupted secret."
                ) from e


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
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp=stamp,
                hashed_secret=hash(secret),
            )

        with pytest.raises(SecretNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp=stamps[0],
                hashed_secret=hash("I love stawberries :)"),
            )

        with pytest.raises(PilotNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp="I am a spider",
                hashed_secret=hash(secrets[0]),
            )


@pytest.mark.parametrize("secret_duration_sec", [1])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret_with_delay(
    pilot_agents_db: PilotAgentsDB,
    add_secrets_and_time,
    frozen_time: freezegun.FreezeGun,
):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    # Move forward few minutes
    frozen_time.tick(delta=timedelta(minutes=5))

    async with pilot_agents_db as pilot_agents_db:
        with pytest.raises(SecretHasExpiredError):
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp=stamps[0],
                hashed_secret=hash(secrets[0]),
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
        await verify_pilot_secret(
            pilot_db=pilot_agents_db,
            pilot_stamp=stamps[0],
            hashed_secret=hash(secrets[0]),
        )

        # Second login, should not work because maxed out at 1 try
        # If the foreign key works, we should have "SecretNotFoundError"
        with pytest.raises(SecretNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp=stamps[0],
                hashed_secret=hash(secrets[0]),
            )


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_login_with_bad_secret(
    pilot_agents_db: PilotAgentsDB, add_secrets_and_time
):

    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    # Pilot1 will try to login with every other pilots's secret
    async with pilot_agents_db as pilot_agents_db:
        for secret in secrets[1:]:
            with pytest.raises(BadPilotCredentialsError):
                await verify_pilot_secret(
                    pilot_db=pilot_agents_db,
                    pilot_stamp=stamps[0],
                    hashed_secret=hash(secret),
                )
