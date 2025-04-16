from __future__ import annotations

from diracx.db.sql import PilotAgentsDB


async def get_pilot_informations_by_reference(
    pilot_db: PilotAgentsDB,
    pilot_job_reference: str,
):
    pilots = await pilot_db.get_pilots_by_references_bulk([pilot_job_reference])

    assert len(pilots) == 1

    return pilots[0]


async def get_pilot_ids_from_references(
    pilot_db: PilotAgentsDB, pilot_references: list[str]
) -> list[int]:
    pilots = await pilot_db.get_pilots_by_references_bulk(refs=pilot_references)

    return [pilot["PilotID"] for pilot in pilots]
