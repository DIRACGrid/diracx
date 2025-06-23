from __future__ import annotations

from datetime import datetime, timedelta, timezone
from secrets import token_hex
from typing import Any

from uuid_utils import uuid7

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    SecretHasExpiredError,
)
from diracx.core.models import (
    PilotAccessTokenPayload,
    PilotRefreshTokenPayload,
    PilotSecretConstraints,
    PilotSecretsInfo,
    TokenResponse,
    TokenType,
)
from diracx.core.settings import AuthSettings
from diracx.core.utils import extract_timestamp_from_uuid7, recursive_dict_merge
from diracx.db.sql import AuthDB, PilotAgentsDB
from diracx.db.sql.utils.functions import raw_hash
from diracx.logic.auth.token import (
    create_token,
    get_token_info_from_refresh_flow,
    insert_refresh_token,
)
from diracx.logic.pilots.query import (
    get_pilots_by_stamp,
    get_secrets_by_hashed_secrets,
)


async def create_raw_secrets(
    n: int,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
    secret_constraint: PilotSecretConstraints,
    pilot_secret_use_count_max: int | None = 1,
    expiration_minutes: int | None = None,
) -> tuple[list[str], list[int]]:
    # Get a random string
    # Can be customized
    random_secrets = [generate_pilot_secret() for _ in range(n)]

    hashed_secrets = [raw_hash(random_secret) for random_secret in random_secrets]

    secret_constraints = {
        hashed_secret: secret_constraint for hashed_secret in hashed_secrets
    }

    # Insert secrets
    await pilot_db.insert_unique_secrets(
        hashed_secrets=hashed_secrets,
        secret_global_use_count_max=pilot_secret_use_count_max,
        secret_constraints=secret_constraints,
    )

    secrets_added = await get_secrets_by_hashed_secrets(
        pilot_db=pilot_db,
        hashed_secrets=hashed_secrets,
        parameters=["SecretUUID"],  # For efficiency
    )

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
    await pilot_db.set_secret_expirations(
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
    secret_constraint: PilotSecretConstraints,
    pilot_secret_use_count_max: int | None = 1,
    expiration_minutes: int | None = None,
) -> list[PilotSecretsInfo]:
    pilot_secrets, expiration_dates_timestamps = await create_raw_secrets(
        n=n,
        pilot_db=pilot_db,
        settings=settings,
        pilot_secret_use_count_max=pilot_secret_use_count_max,
        expiration_minutes=expiration_minutes,
        secret_constraint=secret_constraint,
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
    secrets_obj = await get_secrets_by_hashed_secrets(
        pilot_db=pilot_db,
        hashed_secrets=list(hashed_secrets_to_pilot_stamps_dict.keys()),
        parameters=["SecretConstraints"],  # For efficiency, we don't need more info
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

    await pilot_db.update_pilot_secrets_constraints(
        hashed_secrets_to_pilot_stamps_mapping
    )


async def verify_pilot_credentials(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    pilot_stamp: str,
    pilot_secret: str,
    settings: AuthSettings,
) -> TokenResponse:
    hashed_secret = raw_hash(pilot_secret)

    # 1. Get the pilot
    pilots = await get_pilots_by_stamp(
        pilot_db=pilot_db,
        pilot_stamps=[pilot_stamp],
        parameters=["VO"],
        allow_missing=False
    )
    pilot = dict(pilots[0])

    # 2. Get the secret itself
    secrets = await get_secrets_by_hashed_secrets(
        pilot_db=pilot_db, hashed_secrets=[hashed_secret]
    )
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
    pilot_stamp: str,
    auth_db: AuthDB,
    settings: AuthSettings,
    pilot_db: PilotAgentsDB,
    refresh_token: str | None = None,
) -> TokenResponse:
    pilots = await get_pilots_by_stamp(
        pilot_db=pilot_db, pilot_stamps=[pilot_stamp], parameters=["VO"], allow_missing=False
    )
    pilot = pilots[0]  # Semantic
    vo = pilot["VO"]

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


def generate_pilot_secret() -> str:
    # Can change with time
    return token_hex(32)


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
            refresh_token=refresh_token,
            auth_db=auth_db,
            settings=settings,
            token_type=TokenType.PILOT_TOKEN,
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
