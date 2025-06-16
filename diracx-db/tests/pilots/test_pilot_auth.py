from __future__ import annotations

from datetime import timedelta
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
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.db.sql.utils.functions import raw_hash
from diracx.testing.time import mock_sqlite_time

from .utils import (
    add_secrets_and_time,  # noqa: F401
    add_stamps,  # noqa: F401
    verify_pilot_secret,
)


@pytest.fixture
async def pilot_db() -> AsyncGenerator[PilotAgentsDB, None]:
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


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret(
    pilot_db: PilotAgentsDB,
    add_secrets_and_time,  # noqa: F811
    frozen_time: freezegun.FreezeGun,
):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pairs = list(zip(stamps, secrets))
    # Shuffle it to prove that credentials are well associated
    shuffle(pairs)

    async with pilot_db as pilot_db:
        for stamp, secret in pairs:
            await verify_pilot_secret(
                pilot_db=pilot_db,
                pilot_stamp=stamp,
                hashed_secret=raw_hash(secret),
                frozen_time=frozen_time,
            )

        with pytest.raises(SecretNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_db,
                pilot_stamp=stamps[0],
                hashed_secret=raw_hash("I love stawberries :)"),
                frozen_time=frozen_time,
            )

        with pytest.raises(PilotNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_db,
                pilot_stamp="I am a spider",
                hashed_secret=raw_hash(secrets[0]),
                frozen_time=frozen_time,
            )


@pytest.mark.parametrize("secret_duration_sec", [1])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret_with_delay(
    pilot_db: PilotAgentsDB,
    add_secrets_and_time,  # noqa: F811
    frozen_time: freezegun.FreezeGun,
):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    # Move forward few minutes
    frozen_time.tick(delta=timedelta(minutes=5))

    async with pilot_db as pilot_db:
        with pytest.raises(SecretHasExpiredError):
            await verify_pilot_secret(
                pilot_db=pilot_db,
                pilot_stamp=stamps[0],
                hashed_secret=raw_hash(secrets[0]),
                frozen_time=frozen_time,
            )


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_verify_secret_too_much_secret_use(
    pilot_db: PilotAgentsDB,
    add_secrets_and_time,  # noqa: F811
    frozen_time: freezegun.FreezeGun,
):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    # First login, should work
    async with pilot_db as pilot_db:
        await verify_pilot_secret(
            pilot_db=pilot_db,
            pilot_stamp=stamps[0],
            hashed_secret=raw_hash(secrets[0]),
            frozen_time=frozen_time,
        )

        # Second login, should not work because maxed out at 1 try
        # If the foreign key works, we should have "SecretNotFoundError"
        with pytest.raises(SecretNotFoundError):
            await verify_pilot_secret(
                pilot_db=pilot_db,
                pilot_stamp=stamps[0],
                hashed_secret=raw_hash(secrets[0]),
                frozen_time=frozen_time,
            )


@pytest.mark.parametrize("secret_duration_sec", [10])
@pytest.mark.asyncio
async def test_create_pilot_and_login_with_bad_secret(
    pilot_db: PilotAgentsDB,
    add_secrets_and_time,  # noqa: F811
    frozen_time: freezegun.FreezeGun,
):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    async with pilot_db as pilot_db:
        # Pilot1 will try to login with every other pilots's secret
        for secret in secrets[1:]:
            with pytest.raises(BadPilotCredentialsError):
                await verify_pilot_secret(
                    pilot_db=pilot_db,
                    pilot_stamp=stamps[0],
                    hashed_secret=raw_hash(secret),
                    frozen_time=frozen_time,
                )
