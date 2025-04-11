from __future__ import annotations

from ast import literal_eval
from datetime import timedelta
from secrets import token_hex

from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.settings import AuthSettings
from diracx.db.sql import PilotAgentsDB

# TODO: Move this hash function in diracx-logic, and rename it
from diracx.db.sql.utils.functions import hash


def generate_pilot_secret() -> str:
    # Can change with time
    return token_hex(32)


async def add_pilot_credentials(
    pilot_ids: list[int], pilot_db: PilotAgentsDB, settings: AuthSettings
) -> list[str]:
    # Get a random string
    # Can be customized
    random_secrets = [generate_pilot_secret() for _ in range(len(pilot_ids))]

    hashed_secrets = [hash(random_secret) for random_secret in random_secrets]

    date_added = await pilot_db.add_pilots_credentials(
        pilot_ids=pilot_ids, pilot_hashed_secrets=hashed_secrets
    )

    # If we have millions of pilots to add, can take few seconds / minutes to add
    expiration_dates = [
        date + timedelta(seconds=settings.pilot_secret_expire_seconds)
        for date in date_added
    ]

    # Helps compatibility between sql engines
    await pilot_db.set_pilot_credentials_expiration(
        pilot_ids=pilot_ids,
        pilot_secret_expiration_dates=expiration_dates,  # type: ignore
    )

    return random_secrets


def generate_pilot_scope(pilot: dict) -> str:
    return f"vo:{pilot['VO']} property:LimitedDelegation property:GenericPilot"


async def try_login(
    pilot_reference: str, pilot_db: PilotAgentsDB, pilot_secret: str
) -> None:

    hashed_secret = hash(pilot_secret)

    await pilot_db.verify_pilot_secret(
        pilot_hashed_secret=hashed_secret, pilot_job_reference=pilot_reference
    )


async def register_new_pilots(
    pilot_db: PilotAgentsDB,
    pilot_job_references: list[str],
    vo: str,
    grid_type: str = "Dirac",
    pilot_stamps: dict | None = None,
):
    # [IMPORTANT] Check unicity of pilot references
    # If a pilot already exists, it will undo everything and raise an error
    try:
        await pilot_db.get_pilots_by_references_bulk(refs=pilot_job_references)
        raise PilotAlreadyExistsError(pilot_ref=pilot_job_references)
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
            if not e.detail:
                raise ValueError("e.detail is None")

            pilots_that_already_exist = set(pilot_job_references) - set(
                literal_eval(e.detail)
            )
        except ValueError as e2:
            raise ValueError(
                f"Must be a set string representation, got: {e.detail}"
            ) from e2

        if type(pilots_that_already_exist) is not set:
            raise ValueError(
                f"Must be a set, got {type(pilots_that_already_exist)}: {pilots_that_already_exist}"
            ) from e

        if pilots_that_already_exist:
            raise PilotAlreadyExistsError(
                pilot_ref=str(pilots_that_already_exist)
            ) from e

    await pilot_db.add_pilot_references(
        pilot_refs=pilot_job_references,
        vo=vo,
        grid_type=grid_type,
        pilot_stamps=pilot_stamps,
    )
