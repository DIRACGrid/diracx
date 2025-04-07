from __future__ import annotations

from secrets import token_hex

from diracx.db.sql import PilotAgentsDB

# TODO: Move this hash function in diracx-logic, and rename it
from diracx.db.sql.utils.functions import hash


def generate_pilot_secret() -> str:
    # Can change with time
    return token_hex(32)


async def add_pilot_credentials(pilot_id: int, pilot_db: PilotAgentsDB) -> str:

    # Get a random string
    # Can be customized
    random_secret = generate_pilot_secret()

    hashed_secret = hash(random_secret)

    await pilot_db.add_pilot_credentials(
        pilot_id=pilot_id, pilot_hashed_secret=hashed_secret
    )

    return random_secret


def generate_pilot_scope(pilot: dict) -> str:
    return f"vo:{pilot['VO']}"


async def try_login(
    pilot_reference: str, pilot_db: PilotAgentsDB, pilot_secret: str
) -> None:

    hashed_secret = hash(pilot_secret)

    await pilot_db.verify_pilot_secret(
        pilot_hashed_secret=hashed_secret, pilot_job_reference=pilot_reference
    )
