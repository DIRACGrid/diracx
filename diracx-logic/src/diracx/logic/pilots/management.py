from __future__ import annotations

from diracx.core.models import PilotFieldsMapping
from diracx.db.sql import PilotAgentsDB


async def delete_pilots_bulk(pilot_db: PilotAgentsDB, pilot_stamps: list[str]):
    await pilot_db.delete_pilots_bulk(pilot_stamps)


async def update_pilots_fields(
    pilot_db: PilotAgentsDB, pilot_stamps_to_fields_mapping: list[PilotFieldsMapping]
):
    await pilot_db.update_pilot_fields_bulk(pilot_stamps_to_fields_mapping)


async def associate_pilot_with_jobs(
    pilot_db: PilotAgentsDB, pilot_stamp: str, pilot_jobs_ids: list[int]
):
    await pilot_db.associate_pilot_with_jobs(
        pilot_stamp=pilot_stamp, job_ids=pilot_jobs_ids
    )
