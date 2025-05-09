from __future__ import annotations

from ast import literal_eval
from datetime import datetime, timedelta, timezone
from secrets import token_hex
from typing import Any

from diracx.core.config import Config
from diracx.core.exceptions import (
    BadPilotCredentialsError,
    BadPilotVOError,
    ConfigurationError,
    CredentialsAlreadyExistError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.core.models import PilotCredentialsInfo, PilotSecretsInfo, PilotStampInfo
from diracx.core.settings import AuthSettings
from diracx.db.exceptions import DBInBadStateError
from diracx.db.sql import PilotAgentsDB

# TODO: Move this hash function in diracx-logic, and rename it
from diracx.db.sql.utils.functions import hash


def generate_pilot_secret() -> str:
    # Can change with time
    return token_hex(32)


async def create_credentials(
    n: int,
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
    vo: str | None,
    pilot_secret_use_count_max: int | None = 1,
    expiration_minutes: int | None = None,
):

    # Get a random string
    # Can be customized
    random_secrets = [generate_pilot_secret() for _ in range(n)]

    hashed_secrets = [hash(random_secret) for random_secret in random_secrets]

    # Insert secrets
    await pilot_db.insert_unique_secrets_bulk(
        hashed_secrets=hashed_secrets,
        secret_global_use_count_max=pilot_secret_use_count_max,
        vo=vo,
    )

    secrets_added = await pilot_db.get_secrets_by_hashed_secrets_bulk(hashed_secrets)

    # If we have millions of pilots to add, can take few seconds / minutes to add
    expiration_dates = [
        secret["SecretCreationDate"]
        + timedelta(
            seconds=(
                expiration_minutes * 60
                if expiration_minutes
                else settings.pilot_secret_expire_seconds
            )
        )
        for secret in secrets_added
    ]
    secret_ids = [secret["SecretID"] for secret in secrets_added]

    # Helps compatibility between sql engines
    await pilot_db.set_secret_expirations_bulk(
        secret_ids=secret_ids,
        pilot_secret_expiration_dates=expiration_dates,  # type: ignore
    )

    expiration_dates_timestamps = [
        int(expire_date.timestamp()) for expire_date in expiration_dates
    ]

    return random_secrets, hashed_secrets, expiration_dates_timestamps


async def associate_pilots_with_secrets(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str],
    secrets: list[str] | None = None,
    hashed_secrets: list[str] | None = None,
):

    if not hashed_secrets:
        assert secrets
        hashed_secrets = [hash(secret) for secret in secrets]

    # Get the secret ids to later associate them with pilots
    secrets_obj = await pilot_db.get_secrets_by_hashed_secrets_bulk(hashed_secrets)
    secret_ids = [secret["SecretID"] for secret in secrets_obj]

    if len(secret_ids) == 1:
        secret_ids = secret_ids * len(pilot_stamps)

    # Associates pilots with their secrets
    pilot_to_secret_id_mapping_values = [
        {
            "PilotSecretID": secret_id,
            "PilotStamp": pilot_stamp,
        }
        for pilot_stamp, secret_id in zip(pilot_stamps, secret_ids)
    ]

    # Verify first that pilot can access the secrets
    await verify_that_pilot_can_access_secret_bulk(
        pilot_db=pilot_db,
        pilot_to_secret_id_mapping_values=pilot_to_secret_id_mapping_values,
    )

    await pilot_db.associate_pilots_with_secrets_bulk(pilot_to_secret_id_mapping_values)


async def add_pilot_credentials(
    pilot_stamps: list[str],
    pilot_db: PilotAgentsDB,
    settings: AuthSettings,
    vo: str | None,
    pilot_secret_use_count_max: int = 1,
) -> tuple[list[str], list[int]]:
    random_secrets, hashed_secrets, expiration_dates_timestamps = (
        await create_credentials(
            n=len(pilot_stamps),
            pilot_db=pilot_db,
            settings=settings,
            vo=vo,
            pilot_secret_use_count_max=pilot_secret_use_count_max,
        )
    )

    try:
        await associate_pilots_with_secrets(
            pilot_db=pilot_db, hashed_secrets=hashed_secrets, pilot_stamps=pilot_stamps
        )
    except CredentialsAlreadyExistError as e:
        # Undo everything in case of an error.
        # TODO: Validate in PR
        await pilot_db.conn.rollback()
        raise e

    return random_secrets, expiration_dates_timestamps


def create_pilot_credentials_response(
    pilot_stamps: list[str],
    pilot_secrets: list[str],
    pilot_expiration_dates: list[int],
) -> list[PilotCredentialsInfo]:
    return [
        PilotCredentialsInfo(
            pilot_stamp=pilot_stamp,
            pilot_secret=secret,
            pilot_secret_expires_in=expires_in,
        )
        for pilot_stamp, secret, expires_in in zip(
            pilot_stamps, pilot_secrets, pilot_expiration_dates
        )
    ]


def create_secrets_response(
    pilot_secrets: list[str],
    pilot_expiration_dates: list[int],
) -> list[PilotSecretsInfo]:
    return [
        PilotSecretsInfo(
            pilot_secret=secret,
            pilot_secret_expires_in=expires_in,
        )
        for secret, expires_in in zip(pilot_secrets, pilot_expiration_dates)
    ]


def create_stamp_response(pilot_stamps: list[str]) -> list[PilotStampInfo]:
    return [PilotStampInfo(pilot_stamp=stamp) for stamp in pilot_stamps]


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
    except KeyError:
        try:
            vo_config_registry = config.Registry["Defaults"]
        except KeyError as e:
            raise ConfigurationError(
                f"Given VO ({vo}) and 'Defaults' are not registered in the configuration for the registry"
            ) from e

    if not vo_config_operations:
        raise ConfigurationError("Bad config")

    operation_group = vo_config_operations["GenericPilotGroup"]
    registry_group = vo_config_registry.Groups[operation_group]

    return operation_group, registry_group


def generate_pilot_scope(pilot: dict, config: Config) -> str:

    operation_group, registry_group = get_registry_and_group_configuration(
        config, pilot["VO"]
    )

    properties = " ".join(
        [f"property:{pilot_property}" for pilot_property in registry_group.Properties]
    )

    return f"vo:{pilot['VO']} {properties} group:{operation_group}"


def generate_pilot_sub(pilot: dict, config: Config):

    _, registry_group = get_registry_and_group_configuration(config, pilot["VO"])

    pilot_users = list(registry_group.Users)

    # TODO: Assume we have only one group?
    assert len(pilot_users) == 1
    return pilot_users[0]


async def try_login(
    pilot_stamp: str, pilot_db: PilotAgentsDB, pilot_secret: str
) -> None:

    hashed_secret = hash(pilot_secret)

    # 1. Get the pilot to secret association
    pilots_credentials = await pilot_db.get_pilot_credentials_by_stamp([pilot_stamp])

    # 2. Get the pilot secret itself
    secrets = await pilot_db.get_secrets_by_hashed_secrets_bulk([hashed_secret])
    secret = secrets[0]  # Semantic, assured by fetch_records_bulk_or_raises

    matches = [
        pilot_credential
        for pilot_credential in pilots_credentials
        if secret["SecretID"] == pilot_credential["PilotSecretID"]
    ]

    # 3. Compare the secret_id
    if len(matches) == 0:

        raise BadPilotCredentialsError(
            data={
                "pilot_stamp": pilot_stamp,
                "pilot_hashed_secret": hashed_secret,
                "real_hashed_secret": secret["HashedSecret"],
                "pilot_secret_id[]": str(
                    [
                        pilot_credential["PilotSecretID"]
                        for pilot_credential in pilots_credentials
                    ]
                ),
                "secret_id": secret["SecretID"],
                "test": str(pilots_credentials),
            }
        )
    elif len(matches) > 1:

        raise DBInBadStateError(
            detail="This should not happen. Duplicates in the database."
        )
    pilot_credentials = matches[0]  # Semantic

    # 4. Check if the secret is expired
    now = datetime.now(tz=timezone.utc)
    # Convert the timezone, TODO: Change with #454: https://github.com/DIRACGrid/diracx/pull/454
    expiration = secret["SecretExpirationDate"].replace(tzinfo=timezone.utc)
    if expiration < now:

        try:
            await pilot_db.delete_secrets_bulk([secret["SecretID"]])
        except SecretNotFoundError as e:
            await pilot_db.conn.rollback()

            raise DBInBadStateError(
                detail="This should not happen. Pilot should have a secret, but not found."
            ) from e

        raise SecretHasExpiredError(
            data={
                "pilot_hashed_secret": hashed_secret,
                "now": str(now),
                "expiration_date": secret["SecretExpirationDate"],
            }
        )

    # 5. Now the pilot is authorized, increment the counters (globally and locally).
    try:
        # 5.1 Increment the local count
        await pilot_db.increment_pilot_local_secret_and_last_time_use(
            pilot_secret_id=pilot_credentials["PilotSecretID"],
            pilot_stamp=pilot_credentials["PilotStamp"],
        )

        # 5.2 Increment the global count
        await pilot_db.increment_global_secret_use(
            secret_id=pilot_credentials["PilotSecretID"]
        )
    except Exception as e:  # Generic, to catch it.
        # Should NOT happen
        # Wrapped in a try/catch to still catch in case of an error in the counters
        # Caught and raised here to avoid raising a 4XX error
        await pilot_db.conn.rollback()

        raise DBInBadStateError(
            detail="This should not happen. Pilot has credentials, but has a corrupted secret."
        ) from e

    # 6. Delete all secrets if its count attained the secret_global_use_count_max
    if secret["SecretGlobalUseCountMax"]:
        if secret["SecretGlobalUseCount"] + 1 == secret["SecretGlobalUseCountMax"]:
            try:
                await pilot_db.delete_secrets_bulk([secret["SecretID"]])
            except SecretNotFoundError as e:
                # Should NOT happen
                await pilot_db.conn.rollback()
                raise DBInBadStateError(
                    detail="This should not happen. Pilot has credentials, but has corrupted secret."
                ) from e


async def verify_that_pilot_can_access_secret_bulk(
    pilot_db: PilotAgentsDB, pilot_to_secret_id_mapping_values: list[dict[str, Any]]
):
    # 1. Extract unique pilot_stamps and secret_ids
    pilot_stamps = [entry["PilotStamp"] for entry in pilot_to_secret_id_mapping_values]
    secret_ids = [entry["PilotSecretID"] for entry in pilot_to_secret_id_mapping_values]

    # 2. Bulk fetch pilot and secret info
    pilots = await pilot_db.get_pilots_by_stamp_bulk(pilot_stamps)
    secrets = await pilot_db.get_secrets_by_secret_ids_bulk(secret_ids)

    # 3. Build lookup maps
    pilot_vo_map = {pilot["PilotStamp"]: pilot["VO"] for pilot in pilots}
    secret_vo_map = {secret["SecretID"]: secret["SecretVO"] for secret in secrets}

    # 4. Validate access
    bad_mapping = []

    for mapping in pilot_to_secret_id_mapping_values:
        pilot_stamp = mapping["PilotStamp"]
        secret_id = mapping["PilotSecretID"]

        pilot_vo = pilot_vo_map[pilot_stamp]
        secret_vo = secret_vo_map[secret_id]

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
    grid_type: str = "Dirac",
    pilot_job_references: dict | None = None,
):
    # [IMPORTANT] Check unicity of pilot references
    # If a pilot already exists, it will undo everything and raise an error
    try:
        await pilot_db.get_pilots_by_stamp_bulk(pilot_stamps=pilot_stamps)
        raise PilotAlreadyExistsError(data={"pilot_stamps": str(pilot_stamps)})
    except PilotNotFoundError as e:
        # e.detail is a string representation of the pilot that are not found
        # Ex: e.detail = '{"ref1", "ref2", ...}'
        # We can parse it into a list (literal_eval)
        # We can compare it with the pilot references that want to add
        # If both list are the same, it means that every pilots is new, and so we can add them to the db
        # If not, it means that at least one is already in the db
        # We can verify it with "is not list"

        # To help verification, we transform them temporarily into sets
        # We don't deal with sets every time because they remove the order
        try:
            if isinstance(e.detail, str):
                pilots_that_already_exist = set(pilot_stamps) - set(
                    literal_eval(e.detail)
                )
            else:
                raise ValueError("Bad internal error.")
        except AttributeError as e2:
            raise ValueError("Must be defined and a set string representation") from e2

        if not isinstance(pilots_that_already_exist, set):
            raise ValueError(
                f"Must be a set, got {type(pilots_that_already_exist)}: {pilots_that_already_exist}"
            ) from e

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
