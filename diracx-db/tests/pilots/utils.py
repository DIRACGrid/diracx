from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import freezegun
import pytest
from sqlalchemy import update

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.core.models import (
    PilotSecretConstraints,
    ScalarSearchOperator,
    ScalarSearchSpec,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.db.sql.pilots.schema import PilotAgents
from diracx.db.sql.utils.functions import raw_hash

MAIN_VO = "lhcb"
N = 100

# ------------ Fetching data ------------


async def get_pilots_by_stamp(
    pilot_db: PilotAgentsDB, pilot_stamps: list[str], parameters: list[str] = []
) -> list[dict[Any, Any]]:
    if parameters:
        parameters.append("PilotStamp")
    _, pilots = await pilot_db.search_pilots(
        parameters=parameters,
        search=[
            VectorSearchSpec(
                parameter="PilotStamp",
                operator=VectorSearchOperator.IN,
                values=pilot_stamps,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=1000,
    )

    return pilots


async def get_pilot_jobs_ids_by_pilot_id(
    pilot_db: PilotAgentsDB, pilot_id: int
) -> list[int]:
    _, jobs = await pilot_db.search_pilot_to_job_mapping(
        parameters=["JobID"],
        search=[
            ScalarSearchSpec(
                parameter="PilotID",
                operator=ScalarSearchOperator.EQUAL,
                value=pilot_id,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=10000,
    )

    return [job["JobID"] for job in jobs]


async def get_secrets_by_hashed_secrets(
    pilot_db: PilotAgentsDB, hashed_secrets: list[bytes], parameters: list[str] = []
) -> list[dict[Any, Any]]:
    _, secrets = await pilot_db.search_secrets(
        parameters=parameters,
        search=[
            VectorSearchSpec(
                parameter="HashedSecret",
                operator=VectorSearchOperator.IN,
                values=hashed_secrets,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=1000,
    )

    return secrets


async def get_secrets_by_uuid(
    pilot_db: PilotAgentsDB, secret_uuids: list[str], parameters: list[str] = []
) -> list[dict[Any, Any]]:
    parameters.append("SecretUUID")  # To avoid bug later on `found_keys = ...`

    _, secrets = await pilot_db.search_secrets(
        parameters=parameters,
        search=[
            VectorSearchSpec(
                parameter="SecretUUID",
                operator=VectorSearchOperator.IN,
                values=secret_uuids,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=1000,
    )

    # Custom handling, to see which secret_uuid does not exist
    # TODO: Add missing in the error
    found_keys = {row["SecretUUID"] for row in secrets}
    missing = set(secret_uuids) - found_keys

    if missing:
        raise SecretNotFoundError(
            data={"secret_uuid": str(missing)}, detail=str(missing)
        )

    return secrets


# ------------ Creating data ------------


@pytest.fixture
async def add_stamps(pilot_db):
    async def _add_stamps(start_n=0):
        async with pilot_db as db:
            # Add pilots
            refs = [f"ref_{i}" for i in range(start_n, start_n + N)]
            stamps = [f"stamp_{i}" for i in range(start_n, start_n + N)]
            pilot_references = dict(zip(stamps, refs))

            vo = MAIN_VO

            await db.add_pilots(
                stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
            )

            return await get_pilots_by_stamp(db, stamps)

    return _add_stamps


@pytest.fixture
async def create_timed_pilots(pilot_db, add_stamps):
    async def _create_timed_pilots(
        old_date: datetime, aborted: bool = False, start_n=0
    ):
        # Get pilots
        pilots = await add_stamps(start_n)

        async with pilot_db as db:
            # Update manually their age
            # Collect PilotStamps
            pilot_stamps = [pilot["PilotStamp"] for pilot in pilots]

            stmt = (
                update(PilotAgents)
                .where(PilotAgents.pilot_stamp.in_(pilot_stamps))
                .values(SubmissionTime=old_date)
            )

            if aborted:
                stmt = stmt.values(Status="Aborted")

            res = await db.conn.execute(stmt)
            assert res.rowcount == len(pilot_stamps)

            pilots = await get_pilots_by_stamp(db, pilot_stamps)
            return pilots

    return _create_timed_pilots


@pytest.fixture
async def create_old_pilots_environment(pilot_db, create_timed_pilots):
    non_aborted_recent = await create_timed_pilots(
        datetime(2025, 1, 1, tzinfo=timezone.utc), False, N
    )
    aborted_recent = await create_timed_pilots(
        datetime(2025, 1, 1, tzinfo=timezone.utc), True, 2 * N
    )

    aborted_very_old = await create_timed_pilots(
        datetime(2003, 3, 10, tzinfo=timezone.utc), True, 3 * N
    )
    non_aborted_very_old = await create_timed_pilots(
        datetime(2003, 3, 10, tzinfo=timezone.utc), False, 4 * N
    )

    pilot_number = 4 * N

    assert pilot_number == (
        len(non_aborted_recent)
        + len(aborted_recent)
        + len(aborted_very_old)
        + len(non_aborted_very_old)
    )

    # Phase 0. Verify that we have the right environment
    async with pilot_db as pilot_db:
        # Ensure that we can get every pilot (only get first of each group)
        await get_pilots_by_stamp(pilot_db, [non_aborted_recent[0]["PilotStamp"]])
        await get_pilots_by_stamp(pilot_db, [aborted_recent[0]["PilotStamp"]])
        await get_pilots_by_stamp(pilot_db, [aborted_very_old[0]["PilotStamp"]])
        await get_pilots_by_stamp(pilot_db, [non_aborted_very_old[0]["PilotStamp"]])

    return non_aborted_recent, aborted_recent, non_aborted_very_old, aborted_very_old


@pytest.fixture
async def add_secrets_and_time(
    pilot_db, add_stamps, secret_duration_sec, frozen_time: freezegun.FreezeGun
):
    # Retrieve the stamps from the add_stamps fixture
    stamps = [pilot["PilotStamp"] for pilot in await add_stamps()]

    # Add a VO restriction as well as association with a specific pilot
    secrets = [f"AW0nd3rfulS3cr3t_{str(i)}" for i in range(len(stamps))]
    hashed_secrets = [raw_hash(secret) for secret in secrets]
    constraints = {
        hashed_secret: PilotSecretConstraints(VOs=[MAIN_VO], PilotStamps=[stamp])
        for hashed_secret, stamp in zip(hashed_secrets, stamps)
    }

    async with pilot_db as pilot_db:
        # Add creds
        await pilot_db.insert_unique_secrets(
            hashed_secrets=hashed_secrets, secret_constraints=constraints
        )

        # Associate with pilot
        secrets_obj = await get_secrets_by_hashed_secrets(pilot_db, hashed_secrets)

        assert len(secrets_obj) == len(hashed_secrets) == len(stamps)

        # extract_timestamp_from_uuid7(secret_obj["SecretUUID"]) does not work here
        # See #548
        expiration_date = [
            datetime.now(timezone.utc) + timedelta(seconds=secret_duration_sec)
            for secret_obj in secrets_obj
        ]

        await pilot_db.set_secret_expirations(
            secret_uuids=[secret_obj["SecretUUID"] for secret_obj in secrets_obj],
            pilot_secret_expiration_dates=expiration_date,
        )

        # Return both non-hashed secrets and stamps
        return {"stamps": stamps, "secrets": secrets}


# ------------ Verifying data ------------


async def verify_pilot_secret(
    pilot_stamp: str,
    pilot_db: PilotAgentsDB,
    hashed_secret: bytes,
    frozen_time: freezegun.FreezeGun,
) -> None:
    # 1. Get the pilot
    pilots = await get_pilots_by_stamp(
        pilot_db=pilot_db,
        pilot_stamps=[pilot_stamp],
        parameters=["VO", "PilotStamp"],
    )
    if len(pilots) == 0:
        raise PilotNotFoundError(data={"pilot_stamp": pilot_stamp})
    pilot = dict(pilots[0])

    # 2. Get the secret itself
    secrets = await get_secrets_by_hashed_secrets(
        pilot_db=pilot_db, hashed_secrets=[hashed_secret]
    )
    if len(secrets) == 0:
        raise SecretNotFoundError(data={"hashed_secret": str(hashed_secret)})
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
        await pilot_db.delete_secrets([secret_uuid])

        raise SecretHasExpiredError(
            data={
                "pilot_hashed_secret": str(hashed_secret),
                "now": str(now),
                "expiration_date": secret["SecretExpirationDate"],
            }
        )

    # 5. Now the pilot is authorized, change when the pilot used the secret.
    await pilot_db.update_pilot_secret_use_time(
        secret_uuid=secret_uuid,
    )

    # 6. Delete the secret if its count attained the secret_global_use_count_max
    if secret["SecretRemainingUseCount"]:
        # If we use it another time, SecretRemainingUseCount will be equal to 0 so we can delete it
        if secret["SecretRemainingUseCount"] == 1:
            await pilot_db.delete_secrets([secret_uuid])


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
