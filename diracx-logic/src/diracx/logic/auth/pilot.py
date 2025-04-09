from __future__ import annotations

from datetime import timedelta
from secrets import token_hex

from diracx.core.settings import AuthSettings
from diracx.db.sql import PilotAgentsDB

# TODO: Move this hash function in diracx-logic, and rename it
from diracx.db.sql.utils.functions import hash


def generate_pilot_secret() -> str:
    # Can change with time
    return token_hex(32)


async def add_pilot_credentials(
    pilot_id: int, pilot_db: PilotAgentsDB, settings: AuthSettings
) -> str:

    # Get a random string
    # Can be customized
    random_secret = generate_pilot_secret()

    hashed_secret = hash(random_secret)

    date_added = await pilot_db.add_pilot_credentials(
        pilot_id=pilot_id, pilot_hashed_secret=hashed_secret
    )

    # Helps compatibility between sql engines
    await pilot_db.set_pilot_credentials_expiration(
        pilot_id=pilot_id,
        pilot_secret_expiration_date=date_added  # type: ignore
        + timedelta(seconds=settings.pilot_secret_expire_seconds),
    )

    return random_secret


def generate_pilot_scope(pilot: dict) -> str:
    return f"vo:{pilot['VO']} property:LimitedDelegation property:GenericPilot"


async def try_login(
    pilot_reference: str, pilot_db: PilotAgentsDB, pilot_secret: str
) -> None:

    hashed_secret = hash(pilot_secret)

    await pilot_db.verify_pilot_secret(
        pilot_hashed_secret=hashed_secret, pilot_job_reference=pilot_reference
    )
