from __future__ import annotations

from ast import literal_eval
from datetime import timedelta
from secrets import token_hex

from diracx.core.config import Config
from diracx.core.exceptions import (
    ConfigurationError,
    CredentialsAlreadyExistError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
)
from diracx.core.models import PilotCredentialsInfo, PilotSecretsInfo, PilotStampInfo
from diracx.core.settings import AuthSettings
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
        vo_config_registry = config.Registry[vo]
    except KeyError:
        try:
            vo_config_operations = config.Operations["Default"].Pilot
            vo_config_registry = config.Registry["Default"]
        except KeyError as e:
            raise ConfigurationError(
                f"Given VO ({vo}) and 'Default' are not registered in the configuration"
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

    await pilot_db.verify_pilot_secret(
        pilot_stamp=pilot_stamp, pilot_hashed_secret=hashed_secret
    )


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
        # We don't deal with sets everytime because they remove the order
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
