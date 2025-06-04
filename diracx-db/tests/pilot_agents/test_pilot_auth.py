from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import shuffle
from typing import Any, AsyncGenerator, Generator

import freezegun
import pytest
import sqlalchemy

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.core.models import PilotSecretConstraints
from diracx.db.exceptions import DBInBadStateError
from diracx.db.sql.pilot_agents.db import PilotAgentsDB
from diracx.db.sql.utils.functions import raw_hash
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

        # Add a VO restriction as well as association with a specific pilot
        secrets = [f"AW0nd3rfulS3cr3t_{str(i)}" for i in range(len(stamps))]
        hashed_secrets = [raw_hash(secret) for secret in secrets]
        constraints = {
            hashed_secret: PilotSecretConstraints(VOs=[MAIN_VO], PilotStamps=[stamp])
            for hashed_secret, stamp in zip(hashed_secrets, stamps)
        }

        # Add creds
        await pilot_agents_db.insert_unique_secrets_bulk(
            hashed_secrets=hashed_secrets, secret_constraints=constraints
        )

        # Associate with pilot
        secrets_obj = await pilot_agents_db.get_secrets_by_hashed_secrets_bulk(
            hashed_secrets
        )

        assert len(secrets_obj) == len(hashed_secrets) == len(stamps)

        # extract_timestamp_from_uuid7(secret_obj["SecretUUID"]) does not work here
        # See #548
        expiration_date = [
            datetime.now(timezone.utc) + timedelta(seconds=secret_duration_sec)
            for secret_obj in secrets_obj
        ]

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_uuids=[secret_obj["SecretUUID"] for secret_obj in secrets_obj],
            pilot_secret_expiration_dates=expiration_date,
        )

        # Return both non-hashed secrets and stamps
        return {"stamps": stamps, "secrets": secrets}


async def check_pilot_constraints(
    pilot: dict[str, Any], secret_constraints: PilotSecretConstraints
):
    key_map = {"VOs": "VO", "PilotStamps": "PilotStamp", "Sites": "Site"}

    for constraint_key, pilot_key in key_map.items():
        allowed_values = secret_constraints.get(constraint_key)
        if allowed_values:
            pilot_value = pilot.get(pilot_key)
            if pilot_value is None or pilot_value not in allowed_values:
                raise BadPilotCredentialsError(
                    data={
                        "pilot": str(pilot),
                        "secret_constraints": str(secret_constraints),
                    }
                )


async def verify_pilot_secret(
    pilot_stamp: str,
    pilot_db: PilotAgentsDB,
    hashed_secret: bytes,
    frozen_time: freezegun.FreezeGun,
) -> None:
    # 1. Get the pilot
    pilots = await pilot_db.get_pilots_by_stamp_bulk([pilot_stamp])
    pilot = dict(pilots[0])  # Semantic, assured by fetch_records_bulk_or_raises

    # 2. Get the secret itself
    secrets = await pilot_db.get_secrets_by_hashed_secrets_bulk([hashed_secret])
    secret = secrets[0]
    secret_uuid = secret["SecretUUID"]
    secret_constraints = PilotSecretConstraints(**secret["SecretConstraints"])

    # 3. Check the constraints
    await check_pilot_constraints(pilot=pilot, secret_constraints=secret_constraints)

    # 4. Check if the secret is expired
    now = datetime.now(tz=timezone.utc)
    # Convert the timezone, TODO: Change with #454: https://github.com/DIRACGrid/diracx/pull/454
    expiration = secret["SecretExpirationDate"].replace(tzinfo=timezone.utc)
    if expiration < now:
        try:
            await pilot_db.delete_secrets_bulk([secret_uuid])
        except SecretNotFoundError as e:
            raise DBInBadStateError(
                detail="This should not happen. Pilot should have a secret, but not found."
            ) from e

        raise SecretHasExpiredError(
            data={
                "pilot_hashed_secret": str(hashed_secret),
                "now": str(now),
                "expiration_date": secret["SecretExpirationDate"],
            }
        )

    # 5. Now the pilot is authorized, change when the pilot used the secret.
    try:
        await pilot_db.update_pilot_secret_use_time(
            secret_uuid=secret_uuid,
        )
    except Exception as e:  # Generic, to catch it.
        # Should NOT happen
        # Wrapped in a try/catch to still catch in case of an error in the counters
        # Caught and raised here to avoid raising a 4XX error
        raise DBInBadStateError(
            detail="This should not happen. Pilot has credentials, but has a corrupted secret."
        ) from e

    # 6. Delete the secret if its count attained the secret_global_use_count_max
    if secret["SecretRemainingUseCount"]:
        # If we use it another time, SecretRemainingUseCount will be equal to 0 so we can delete it
        if secret["SecretRemainingUseCount"] == 1:
            try:
                await pilot_db.delete_secrets_bulk([secret_uuid])
            except SecretNotFoundError as e:
                # Should NOT happen
                raise DBInBadStateError(
                    detail="This should not happen. Pilot has credentials, but has corrupted secret."
                ) from e


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret(
    pilot_agents_db: PilotAgentsDB,
    add_secrets_and_time,
    frozen_time: freezegun.FreezeGun,
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
                hashed_secret=raw_hash(secret),
                frozen_time=frozen_time,
            )

        with pytest.raises(SecretNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp=stamps[0],
                hashed_secret=raw_hash("I love stawberries :)"),
                frozen_time=frozen_time,
            )

        with pytest.raises(PilotNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp="I am a spider",
                hashed_secret=raw_hash(secrets[0]),
                frozen_time=frozen_time,
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
                hashed_secret=raw_hash(secrets[0]),
                frozen_time=frozen_time,
            )


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret_too_much_secret_use(
    pilot_agents_db: PilotAgentsDB,
    add_secrets_and_time,
    frozen_time: freezegun.FreezeGun,
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
            hashed_secret=raw_hash(secrets[0]),
            frozen_time=frozen_time,
        )

        # Second login, should not work because maxed out at 1 try
        # If the foreign key works, we should have "SecretNotFoundError"
        with pytest.raises(SecretNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_agents_db,
                pilot_stamp=stamps[0],
                hashed_secret=raw_hash(secrets[0]),
                frozen_time=frozen_time,
            )


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_login_with_bad_secret(
    pilot_agents_db: PilotAgentsDB,
    add_secrets_and_time,
    frozen_time: freezegun.FreezeGun,
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
                    hashed_secret=raw_hash(secret),
                    frozen_time=frozen_time,
                )
