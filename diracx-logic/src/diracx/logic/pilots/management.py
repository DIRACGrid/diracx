from __future__ import annotations

from typing import Any

from diracx.core.models import PilotFieldsMapping, SearchParams
from diracx.db.sql import PilotAgentsDB

MAX_PER_PAGE = 10000


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


async def get_pilot_info(
    pilot_db: PilotAgentsDB,
    page: int = 1,
    per_page: int = 100,
    body: SearchParams | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Retrieve information about jobs."""
    # Apply a limit to per_page to prevent abuse of the API
    if per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

    if body is None:
        body = SearchParams()

    total, jobs = await pilot_db.search(
        body.parameters,
        body.search,
        body.sort,
        distinct=body.distinct,
        page=page,
        per_page=per_page,
    )

    return total, jobs
