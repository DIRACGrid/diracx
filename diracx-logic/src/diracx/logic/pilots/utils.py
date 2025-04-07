from __future__ import annotations

from diracx.db.sql import PilotAgentsDB


async def get_pilot_informations_by_reference(
    pilot_db: PilotAgentsDB,
    pilot_job_reference: str,
):
    pilot = await pilot_db.get_pilot_by_reference(pilot_ref=pilot_job_reference)

    return pilot
