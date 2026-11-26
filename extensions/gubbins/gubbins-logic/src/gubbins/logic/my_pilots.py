"""Logic of the example managing pilot submissions to compute elements."""

from __future__ import annotations

__all__ = [
    "get_available_ces",
    "get_pilot_summary",
    "submit_pilot",
    "transition_pilot_states",
    "PilotSubmissionError",
]

import random

from gubbins.db.sql.my_pilot_db.db import MyPilotDB
from gubbins.db.sql.my_pilot_db.schema import MyPilotStatus


class PilotSubmissionError(Exception):
    """Raised when the submission of a pilot to a compute element fails."""

    def __init__(self, ce_name: str, success_rate: float):
        super().__init__(
            f"Pilot submission to {ce_name} failed (success_rate={success_rate})"
        )


async def submit_pilot(my_pilot_db: MyPilotDB, ce_name: str) -> int:
    """Submit a single pilot to a CE, raising on probabilistic failure.

    Args:
        my_pilot_db: Pilot database.
        ce_name: Name of the compute element.

    Returns:
        The ID of the new pilot submission.

    Raises:
        PilotSubmissionError: If the submission fails, with a probability
            given by the success rate of the compute element.
    """
    success_rate = await my_pilot_db.get_ce_success_rate(ce_name)
    if random.random() >= success_rate:
        raise PilotSubmissionError(ce_name, success_rate)
    return await my_pilot_db.submit_pilot(ce_name)


async def transition_pilot_states(my_pilot_db: MyPilotDB) -> None:
    """Move pilots through their lifecycle based on CE success rates.

    Submitted pilots become running. Running pilots become done or failed,
    according to the success rate of their compute element.

    Args:
        my_pilot_db: Pilot database.
    """
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
    """Return CEs with available pilot slots.

    Args:
        my_pilot_db: Pilot database.

    Returns:
        For each compute element with free capacity, its name, capacity,
        success rate and number of available slots.
    """
    return await my_pilot_db.get_available_ces()


async def get_pilot_summary(my_pilot_db: MyPilotDB) -> dict[str, int]:
    """Return aggregate pilot statistics across all VOs.

    Args:
        my_pilot_db: Pilot database.

    Returns:
        The number of pilots for each status.
    """
    return await my_pilot_db.get_pilot_summary()
