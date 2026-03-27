from __future__ import annotations

import random

from gubbins.db.sql.my_pilot_db.db import MyPilotDB
from gubbins.db.sql.my_pilot_db.schema import MyPilotStatus


class PilotSubmissionError(Exception):
    def __init__(self, ce_name: str, success_rate: float):
        super().__init__(
            f"Pilot submission to {ce_name} failed (success_rate={success_rate})"
        )


async def submit_pilot(my_pilot_db: MyPilotDB, ce_name: str) -> int:
    """Submit a single pilot to a CE, raising on probabilistic failure."""
    success_rate = await my_pilot_db.get_ce_success_rate(ce_name)
    if random.random() >= success_rate:
        raise PilotSubmissionError(ce_name, success_rate)
    return await my_pilot_db.submit_pilot(ce_name)


async def transition_pilot_states(my_pilot_db: MyPilotDB) -> None:
    """Move pilots through their lifecycle based on CE success rates."""
    submitted = await my_pilot_db.get_pilots_by_status(MyPilotStatus.SUBMITTED)
    for pilot in submitted:
        await my_pilot_db.update_pilot_status(pilot["pilot_id"], MyPilotStatus.RUNNING)

    running = await my_pilot_db.get_pilots_by_status(MyPilotStatus.RUNNING)
    for pilot in running:
        success_rate = await my_pilot_db.get_ce_success_rate(pilot["ce_name"])
        new_status = (
            MyPilotStatus.DONE
            if random.random() < success_rate
            else MyPilotStatus.FAILED
        )
        await my_pilot_db.update_pilot_status(pilot["pilot_id"], new_status)


async def get_available_ces(my_pilot_db: MyPilotDB) -> list[dict]:
    """Return CEs with available pilot slots."""
    return await my_pilot_db.get_available_ces()


async def get_pilot_summary(my_pilot_db: MyPilotDB) -> dict[str, int]:
    """Return aggregate pilot statistics across all VOs."""
    return await my_pilot_db.get_pilot_summary()
