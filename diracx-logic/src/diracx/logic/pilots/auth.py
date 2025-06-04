from __future__ import annotations

from datetime import datetime, timedelta, timezone
from secrets import token_hex
from typing import Any

from uuid_utils import uuid7

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.core.models import (
    PilotAccessTokenPayload,
    PilotCredentialsInfo,
    PilotRefreshTokenPayload,
    PilotSecretConstraints,
    PilotSecretsInfo,
    PilotStampInfo,
    TokenResponse,
)
from diracx.core.settings import AuthSettings
from diracx.core.utils import extract_timestamp_from_uuid7, recursive_dict_merge
from diracx.db.exceptions import DBInBadStateError
from diracx.db.sql import AuthDB, PilotAgentsDB

# TODO: Move this hash function in diracx-logic, and rename it
from diracx.db.sql.utils.functions import raw_hash
from diracx.logic.auth.token import (
    create_token,
    get_token_info_from_refresh_flow,
    insert_refresh_token,
)


def generate_pilot_secret() -> str:
    # Can change with time
    return token_hex(32)


async def serialize_tokens(
    access_token_payload: PilotAccessTokenPayload,
    refresh_token_payload: PilotRefreshTokenPayload | None,
    settings: AuthSettings,
):
    access_token = create_token(payload=access_token_payload, settings=settings)

    refresh_token: str | None = None
    if refresh_token_payload:
        refresh_token = create_token(payload=refresh_token_payload, settings=settings)

    return TokenResponse(
        access_token=access_token,
        expires_in=settings.pilot_secret_expire_seconds,
        refresh_token=refresh_token,
    )


async def create_raw_secrets(
    n: int,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
    pilot_secret_use_count_max: int | None = 1,
    expiration_minutes: int | None = None,
    secret_constraints: dict[bytes, PilotSecretConstraints] = {},
) -> tuple[list[str], list[int]]:
    # Get a random string
    # Can be customized
    random_secrets = [generate_pilot_secret() for _ in range(n)]

    hashed_secrets = [raw_hash(random_secret) for random_secret in random_secrets]

    # Insert secrets
    await pilot_db.insert_unique_secrets_bulk(
        hashed_secrets=hashed_secrets,
        secret_global_use_count_max=pilot_secret_use_count_max,
        secret_constraints=secret_constraints,
    )

    secrets_added = await pilot_db.get_secrets_by_hashed_secrets_bulk(hashed_secrets)

    # If we have millions of pilots to add, can take few seconds / minutes to add
    expiration_dates = [
        extract_timestamp_from_uuid7(secret["SecretUUID"])
        + timedelta(
            seconds=(
                expiration_minutes * 60
                if expiration_minutes
                else settings.pilot_secret_expire_seconds
            )
        )
        for secret in secrets_added
    ]
    secret_uuids = [secret["SecretUUID"] for secret in secrets_added]

    # Helps compatibility between sql engines
    await pilot_db.set_secret_expirations_bulk(
        secret_uuids=secret_uuids,
        pilot_secret_expiration_dates=expiration_dates,  # type: ignore
    )

    expiration_dates_timestamps = [
        int(expire_date.timestamp()) for expire_date in expiration_dates
    ]

    return random_secrets, expiration_dates_timestamps


async def create_secrets(
    n: int,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
    pilot_secret_use_count_max: int | None = 1,
    expiration_minutes: int | None = None,
    secret_constraints: dict[bytes, PilotSecretConstraints] = {},
) -> list[PilotSecretsInfo]:
    pilot_secrets, expiration_dates_timestamps = await create_raw_secrets(
        n=n,
        pilot_db=pilot_db,
        settings=settings,
        pilot_secret_use_count_max=pilot_secret_use_count_max,
        expiration_minutes=expiration_minutes,
        secret_constraints=secret_constraints,
    )

    return [
        PilotSecretsInfo(
            pilot_secret=secret,
            pilot_secret_expires_in=expires_in,
        )
        for secret, expires_in in zip(pilot_secrets, expiration_dates_timestamps)
    ]


async def update_secrets_constraints(
    pilot_db: PilotAgentsDB,
    secrets_to_constraints_dict: dict[str, PilotSecretConstraints],
):
    # 1. Create a mapping that uses hashed_secret
    # Modify the mapping to use hashed_secrets instead of secrets
    hashed_secrets_to_pilot_stamps_dict = {
        raw_hash(secret): constraints
        for secret, constraints in secrets_to_constraints_dict.items()
    }
    # Now the dictionary follows: {b"<hashed_secret>": [<list of stamps associated with it>]}
    #
    # If we had a list like so : [{"b_...UUID": <uuid>, "b_...Stamp": <stamp>}], to update the JSON we would need
    # to groupby it if we use multiple times the same secret (to modify the JSON by merging and not overriding)

    # 2. Get the secret ids to later associate them with pilots
    # It also verifies that all secrets exist
    secrets_obj = await pilot_db.get_secrets_by_hashed_secrets_bulk(
        list(hashed_secrets_to_pilot_stamps_dict.keys())
    )

    # Mapping [ {"PilotHashedSecret": b"<hashed_secret>", "PilotSecretConstraints": {...}} ]
    # This is useful to update inside the database, but it is not useful to merge the old JSON with the new one
    hashed_secrets_to_pilot_stamps_mapping: list[dict[str, Any]] = []

    # 3. Merge the constraints so that we don't loose the old ones
    for secret_obj in secrets_obj:
        # Get the current constraints and hashed_secret
        secret_constraints = PilotSecretConstraints(**secret_obj["SecretConstraints"])
        hashed_secret = secret_obj["HashedSecret"]

        # Merge it with the given constraints
        new_secret_constraints = hashed_secrets_to_pilot_stamps_dict[hashed_secret]

        hashed_secrets_to_pilot_stamps_mapping.append(
            {
                "PilotHashedSecret": hashed_secret,
                "PilotSecretConstraints": recursive_dict_merge(
                    secret_constraints, new_secret_constraints
                ),
            }
        )

    await pilot_db.update_pilot_secrets_constraints_bulk(
        hashed_secrets_to_pilot_stamps_mapping
    )


async def generate_pilot_tokens(
    vo: str,
    pilot_stamp: str,
    auth_db: AuthDB,
    settings: AuthSettings,
    refresh_token: str | None = None,
) -> tuple[PilotAccessTokenPayload, PilotRefreshTokenPayload | None]:
    include_refresh_token = True

    if refresh_token is not None:
        (
            pilot_info,
            scope,
            legacy_exchange,
            _,
            include_refresh_token,
        ) = await get_token_info_from_refresh_flow(
            refresh_token=refresh_token, auth_db=auth_db, settings=settings
        )

        sub = f"{vo}:{pilot_info['sub']}"
    else:
        # We don't need a user sub as before
        # https://github.com/DIRACGrid/diracx/pull/421#issuecomment-2909087954
        # Same for the property, but it is useful as we store the scope (to detect pilots)
        scope = f"vo:{vo} property:GenericPilot"
        sub = f"{vo}:{pilot_stamp}"
        legacy_exchange = False

    return await exchange_token(
        scope=scope,
        sub=sub,
        vo=vo,
        pilot_stamp=pilot_stamp,
        auth_db=auth_db,
        settings=settings,
        legacy_exchange=legacy_exchange,
        include_refresh_token=include_refresh_token,
    )


async def check_pilot_constraints(
    pilot: dict[str, Any], secret_constraints: PilotSecretConstraints
):
    key_map = {"VOs": "VO", "PilotStamps": "PilotStamp", "Sites": "Site"}

    err = BadPilotCredentialsError(
        data={
            "pilot": str(pilot),
            "secret_constraints": str(secret_constraints),
        }
    )

    for constraint_key, pilot_key in key_map.items():
        expected = secret_constraints.get(constraint_key)
        if expected is not None:
            pilot_value = pilot.get(pilot_key)
            if pilot_value is None:
                raise err

            if isinstance(expected, list):
                if pilot_value not in expected:
                    raise err
            else:
                if pilot_value != expected:
                    raise err


async def verify_pilot_credentials(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    pilot_stamp: str,
    pilot_secret: str,
    settings: AuthSettings,
) -> TokenResponse:
    hashed_secret = raw_hash(pilot_secret)

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

    # Get token, and serialize
    access_token_payload, refresh_token_payload = await generate_pilot_tokens(
        vo=pilot["VO"],
        pilot_stamp=pilot_stamp,
        auth_db=auth_db,
        settings=settings,
        refresh_token=None,
    )

    return await serialize_tokens(
        access_token_payload=access_token_payload,
        refresh_token_payload=refresh_token_payload,
        settings=settings,
    )


async def refresh_pilot_token(
    vo: str,
    pilot_stamp: str,
    auth_db: AuthDB,
    settings: AuthSettings,
    refresh_token: str | None = None,
) -> TokenResponse:
    access_token_payload, refresh_token_payload = await generate_pilot_tokens(
        vo=vo,
        pilot_stamp=pilot_stamp,
        auth_db=auth_db,
        settings=settings,
        refresh_token=refresh_token,
    )

    return await serialize_tokens(
        access_token_payload=access_token_payload,
        refresh_token_payload=refresh_token_payload,
        settings=settings,
    )


async def register_new_pilots(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str],
    vo: str,
    pilot_secret_use_count_max: int,
    settings: AuthSettings,
    generate_secrets: bool,
    grid_type: str = "Dirac",
    pilot_job_references: dict | None = None,
) -> list[PilotStampInfo] | list[PilotCredentialsInfo]:
    # [IMPORTANT] Check unicity of pilot references
    # If a pilot already exists, it will undo everything and raise an error
    try:
        await pilot_db.get_pilots_by_stamp_bulk(pilot_stamps=pilot_stamps)
        raise PilotAlreadyExistsError(data={"pilot_stamps": str(pilot_stamps)})
    except PilotNotFoundError as e:
        # e.non_existing_pilots is set of the pilot that are not found
        # We can compare it with the pilot references that want to add
        # If both sets are the same, it means that every pilots is new, and so we can add them to the db
        # If not, it means that at least one is already in the db

        non_existing_pilots = e.non_existing_pilots
        pilots_that_already_exist = set(pilot_stamps) - non_existing_pilots

        if pilots_that_already_exist:
            raise PilotAlreadyExistsError(
                data={"pilot_stamps": str(pilots_that_already_exist)}
            ) from e

    await pilot_db.add_pilots_bulk(
        pilot_stamps=pilot_stamps,
        vo=vo,
        grid_type=grid_type,
        pilot_references=pilot_job_references,
    )

    if generate_secrets:
        pilot_secrets, expiration_dates_timestamps = await create_raw_secrets(
            n=len(pilot_stamps),
            pilot_db=pilot_db,
            settings=settings,
            pilot_secret_use_count_max=pilot_secret_use_count_max,
        )

        constraints = {
            pilot_secret: PilotSecretConstraints(PilotStamps=[pilot_stamp], VOs=[vo])
            for pilot_secret, pilot_stamp in zip(pilot_secrets, pilot_stamps)
        }

        await update_secrets_constraints(
            pilot_db=pilot_db, secrets_to_constraints_dict=constraints
        )

        return [
            PilotCredentialsInfo(
                pilot_stamp=pilot_stamp,
                pilot_secret=secret,
                pilot_secret_expires_in=expires_in,
            )
            for pilot_stamp, secret, expires_in in zip(
                pilot_stamps, pilot_secrets, expiration_dates_timestamps
            )
        ]

    else:
        return [PilotStampInfo(pilot_stamp=stamp) for stamp in pilot_stamps]


async def exchange_token(
    scope: str,
    sub: str,
    vo: str,
    pilot_stamp: str,
    auth_db: AuthDB,
    settings: AuthSettings,
    legacy_exchange: bool,
    include_refresh_token: bool,
) -> tuple[PilotAccessTokenPayload, PilotRefreshTokenPayload | None]:
    """Method called to exchange the OIDC token for a DIRAC generated access token."""
    # Merge the VO with the stamp to get a sub

    creation_time = datetime.now(timezone.utc)
    # Insert the refresh token with user details into the RefreshTokens table
    # User details are needed to regenerate access tokens later
    jti, creation_time = await insert_refresh_token(
        auth_db=auth_db,
        subject=sub,
        scope=scope,
    )

    refresh_payload: PilotRefreshTokenPayload | None = None
    if include_refresh_token:
        refresh_payload = {
            "jti": str(jti),
            "exp": creation_time
            + timedelta(hours=settings.pilot_refresh_token_expire_hours),
            "legacy_exchange": legacy_exchange,
        }

    # Generate access token payload
    access_payload: PilotAccessTokenPayload = {
        "sub": sub,
        "vo": vo,
        "iss": settings.token_issuer,
        "jti": str(uuid7()),
        # This field is redundant, but if later we change the sub, we won't need to change how we use the token
        "pilot_stamp": pilot_stamp,
        "exp": creation_time + timedelta(minutes=settings.access_token_expire_minutes),
    }

    return access_payload, refresh_payload
