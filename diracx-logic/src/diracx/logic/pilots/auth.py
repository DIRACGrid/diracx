from __future__ import annotations

from datetime import datetime, timedelta, timezone
from secrets import token_hex
from typing import Any

from diracx.core.config import Config
from diracx.core.exceptions import (
    BadPilotCredentialsError,
    BadPilotVOError,
    ConfigurationError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.core.models import (
    AccessTokenPayload,
    PilotCredentialsInfo,
    PilotSecretsInfo,
    PilotStampInfo,
    RefreshTokenPayload,
)
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings
from diracx.core.utils import extract_timestamp_from_uuid7
from diracx.db.exceptions import DBInBadStateError
from diracx.db.sql import AuthDB, PilotAgentsDB

# TODO: Move this hash function in diracx-logic, and rename it
from diracx.db.sql.utils.functions import hash
from diracx.logic.auth.token import exchange_token, get_token_info_from_refresh_flow


def generate_pilot_secret() -> str:
    # Can change with time
    return token_hex(32)


async def create_raw_secrets(
    n: int,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
    vo: str | None,
    pilot_secret_use_count_max: int | None = 1,
    expiration_minutes: int | None = None,
) -> tuple[list[str], list[str], list[int]]:

    # Get a random string
    # Can be customized
    random_secrets = [generate_pilot_secret() for _ in range(n)]

    hashed_secrets = [hash(random_secret).encode() for random_secret in random_secrets]

    # Insert secrets
    await pilot_db.insert_unique_secrets_bulk(
        hashed_secrets=hashed_secrets,
        secret_global_use_count_max=pilot_secret_use_count_max,
        vo=vo,
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

    return random_secrets, hashed_secrets, expiration_dates_timestamps


async def create_secrets(
    n: int,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
    vo: str | None,
    pilot_secret_use_count_max: int | None = 1,
    expiration_minutes: int | None = None,
) -> list[PilotSecretsInfo]:

    pilot_secrets, _, expiration_dates_timestamps = await create_raw_secrets(
        n=n,
        pilot_db=pilot_db,
        settings=settings,
        vo=vo,
        pilot_secret_use_count_max=pilot_secret_use_count_max,
        expiration_minutes=expiration_minutes,
    )

    return [
        PilotSecretsInfo(
            pilot_secret=secret,
            pilot_secret_expires_in=expires_in,
        )
        for secret, expires_in in zip(pilot_secrets, expiration_dates_timestamps)
    ]


async def associate_pilots_with_secrets(
    pilot_db: PilotAgentsDB, pilot_stamps: list[str], pilot_secrets: list[str]
):

    # 1. Hash the secrets
    hashed_secrets = [hash(secret).encode() for secret in pilot_secrets]

    # 2. Get the secret ids to later associate them with pilots
    secrets_obj = await pilot_db.get_secrets_by_hashed_secrets_bulk(hashed_secrets)
    secret_uuids = [secret["SecretUUID"] for secret in secrets_obj]

    if len(secret_uuids) == 1:
        secret_uuids = secret_uuids * len(pilot_stamps)

    # Associates pilots with their secrets
    pilot_to_secret_uuid_mapping_values = [
        {
            "b_PilotSecretUUID": secret_uuid,
            "b_PilotStamp": pilot_stamp,
        }
        for pilot_stamp, secret_uuid in zip(pilot_stamps, secret_uuids)
    ]

    # Verify first that pilot can access the secrets
    await verify_that_pilot_can_access_secret_bulk(
        pilot_db=pilot_db,
        pilot_to_secret_uuid_mapping_values=pilot_to_secret_uuid_mapping_values,
    )

    await pilot_db.associate_pilots_with_secrets_bulk(
        pilot_to_secret_uuid_mapping_values
    )


def get_registry_and_group_configuration(config: Config, vo: str):
    try:
        vo_config_operations = config.Operations[vo].Pilot
    except KeyError:
        try:
            vo_config_operations = config.Operations["Defaults"].Pilot
        except KeyError as e:
            raise ConfigurationError(
                f"Given VO ({vo}) and 'Defaults' are not registered in the configuration for the Pilot"
            ) from e

    try:
        vo_config_registry = config.Registry[vo]
    except KeyError as e:
        raise ConfigurationError(
            f"Given VO ({vo}) is not registered in the configuration for the registry"
        ) from e

    if not vo_config_operations:
        raise ConfigurationError("Bad config")

    return vo_config_operations, vo_config_registry


def generate_pilot_info_for_tokens(vo: str, config: Config) -> tuple[str, str, str]:
    # Get the configuration
    vo_config_operations, vo_config_registry = get_registry_and_group_configuration(
        config, vo
    )
    pilot_group_name = vo_config_operations["GenericPilotGroup"]
    pilot_username = vo_config_operations["GenericPilotUser"]

    registry_group = vo_config_registry.Groups[pilot_group_name]

    # Create the scope
    properties = " ".join(
        [f"property:{pilot_property}" for pilot_property in registry_group.Properties]
    )
    pilot_scope = f"vo:{vo} {properties} group:{pilot_group_name}"

    # Create the sub
    pilots_subs = list(registry_group.Users)
    assert len(pilots_subs) == 1
    pilot_sub = pilots_subs[0]

    return pilot_scope, pilot_sub, pilot_username


async def generate_pilot_tokens(
    vo: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
    available_properties: set[SecurityProperty],
    refresh_token: str | None = None,
) -> tuple[AccessTokenPayload, RefreshTokenPayload | None]:

    scope = None
    pilot_info = None

    if refresh_token is not None:
        (
            pilot_info,
            scope,
            legacy_exchange,
            refresh_token_expire_minutes,
            include_refresh_token,
        ) = await get_token_info_from_refresh_flow(
            refresh_token=refresh_token, auth_db=auth_db, settings=settings
        )
    else:

        scope, sub, pilot_username = generate_pilot_info_for_tokens(
            vo=vo, config=config
        )

        pilot_info = {"preferred_username": pilot_username, "sub": sub}

        refresh_token_expire_minutes = None
        include_refresh_token = True
        legacy_exchange = False

    return await exchange_token(
        auth_db=auth_db,
        scope=scope,
        oidc_token_info=pilot_info,  # TODO: Rename oidc
        config=config,
        settings=settings,
        available_properties=available_properties,
        legacy_exchange=legacy_exchange,
        refresh_token_expire_minutes=refresh_token_expire_minutes,
        include_refresh_token=include_refresh_token,
    )


async def verify_pilot_credentials(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    pilot_stamp: str,
    pilot_secret: str,
    config: Config,
    settings: AuthSettings,
    available_properties: set[SecurityProperty],
) -> tuple[AccessTokenPayload, RefreshTokenPayload | None]:

    hashed_secret = hash(pilot_secret)

    # 1. Get the pilot
    pilots = await pilot_db.get_pilots_by_stamp_bulk([pilot_stamp])
    pilot = dict(pilots[0])  # Semantic, assured by fetch_records_bulk_or_raises
    real_secret_uuid = pilot["PilotSecretUUID"]

    # 2. Get the secret itself
    given_secrets = await pilot_db.get_secrets_by_hashed_secrets_bulk(
        [hashed_secret.encode()]
    )
    given_secret = given_secrets[0]
    given_secret_uuid = given_secret[
        "SecretUUID"
    ]  # Semantic, assured by fetch_records_bulk_or_raises

    # 3. Compare the secret_uuid
    # If SecretUUID is NULL
    if not real_secret_uuid or given_secret_uuid != real_secret_uuid:
        raise BadPilotCredentialsError(
            data={
                "pilot_stamp": pilot_stamp,
                "pilot_hashed_secret": hashed_secret,
                "real_secret_uuid": str(real_secret_uuid),
                "given_secret_uuid": str(given_secret_uuid),
            }
        )

    # 4. Check if the secret is expired
    now = datetime.now(tz=timezone.utc)
    # Convert the timezone, TODO: Change with #454: https://github.com/DIRACGrid/diracx/pull/454
    expiration = given_secret["SecretExpirationDate"].replace(tzinfo=timezone.utc)
    if expiration < now:

        try:
            await pilot_db.delete_secrets_bulk([real_secret_uuid])
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
                await pilot_db.delete_secrets_bulk([given_secret_uuid])
            except SecretNotFoundError as e:
                # Should NOT happen
                raise DBInBadStateError(
                    detail="This should not happen. Pilot has credentials, but has corrupted secret."
                ) from e

    # Get token, and serialize
    return await generate_pilot_tokens(
        vo=pilot["VO"],
        auth_db=auth_db,
        config=config,
        settings=settings,
        available_properties=available_properties,
        refresh_token=None,
    )


async def refresh_pilot_token(
    vo: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
    available_properties: set[SecurityProperty],
    refresh_token: str | None = None,
) -> tuple[AccessTokenPayload, RefreshTokenPayload | None]:

    return await generate_pilot_tokens(
        vo=vo,
        auth_db=auth_db,
        config=config,
        settings=settings,
        available_properties=available_properties,
        refresh_token=refresh_token,
    )


async def verify_that_pilot_can_access_secret_bulk(
    pilot_db: PilotAgentsDB, pilot_to_secret_uuid_mapping_values: list[dict[str, Any]]
):
    # 1. Extract unique pilot_stamps and secret_uuids
    pilot_stamps = [
        entry["b_PilotStamp"] for entry in pilot_to_secret_uuid_mapping_values
    ]
    secret_uuids = [
        entry["b_PilotSecretUUID"] for entry in pilot_to_secret_uuid_mapping_values
    ]

    # 2. Bulk fetch pilot and secret info
    pilots = await pilot_db.get_pilots_by_stamp_bulk(pilot_stamps)
    secrets = await pilot_db.get_secrets_by_secret_uuids_bulk(secret_uuids)

    # 3. Build lookup maps
    pilot_vo_map = {pilot["PilotStamp"]: pilot["VO"] for pilot in pilots}
    secret_vo_map = {secret["SecretUUID"]: secret["SecretVO"] for secret in secrets}

    # 4. Validate access
    bad_mapping = []

    for mapping in pilot_to_secret_uuid_mapping_values:
        pilot_stamp = mapping["b_PilotStamp"]
        secret_uuid = mapping["b_PilotSecretUUID"]

        pilot_vo = pilot_vo_map[pilot_stamp]
        secret_vo = secret_vo_map[secret_uuid]

        # If secret_vo is set to NULL, everybody can access it
        if not secret_vo:
            continue

        # Access allowed only if VOs match or secret_vo is open (None)
        if secret_vo is not None and pilot_vo != secret_vo:
            bad_mapping.append(
                {
                    "pilot_stamp": pilot_stamp,
                    "given_vo": pilot_vo,
                    "expected_vo": secret_vo,
                }
            )

    if bad_mapping:
        raise BadPilotVOError(data={"bad_mapping": str(bad_mapping)})


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

        pilot_secrets, _, expiration_dates_timestamps = await create_raw_secrets(
            n=len(pilot_stamps),
            pilot_db=pilot_db,
            settings=settings,
            vo=vo,
            pilot_secret_use_count_max=pilot_secret_use_count_max,
        )

        await associate_pilots_with_secrets(
            pilot_db=pilot_db, pilot_stamps=pilot_stamps, pilot_secrets=pilot_secrets
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
