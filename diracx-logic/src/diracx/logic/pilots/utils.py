from __future__ import annotations

from diracx.db.sql import PilotAgentsDB


async def get_pilot_information_by_stamps(
    pilot_db: PilotAgentsDB,
    pilot_stamp: str,
):
    pilots = await pilot_db.get_pilots_by_stamp_bulk([pilot_stamp])

    assert len(pilots) == 1

    return pilots[0]


async def get_pilot_ids_from_stamps(
    pilot_db: PilotAgentsDB, pilot_stamps: list[str]
) -> list[int]:
    pilots = await pilot_db.get_pilots_by_stamp_bulk(pilot_stamps=pilot_stamps)

    return [pilot["PilotID"] for pilot in pilots]
