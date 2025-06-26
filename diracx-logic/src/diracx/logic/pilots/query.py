from __future__ import annotations

from typing import Any

from diracx.core.exceptions import PilotNotFoundError
from diracx.core.models import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    SearchParams,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql import PilotAgentsDB

MAX_PER_PAGE = 10000


async def search(
    pilot_db: PilotAgentsDB,
    user_vo: str,
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

    body.search.append(
        ScalarSearchSpec(
            parameter="VO", operator=ScalarSearchOperator.EQUAL, value=user_vo
        )
    )

    total, pilots = await pilot_db.search_pilots(
        body.parameters,
        body.search,
        body.sort,
        distinct=body.distinct,
        page=page,
        per_page=per_page,
    )

    return total, pilots


async def get_pilots_by_stamp(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str],
    parameters: list[str] = [],
    allow_missing: bool = True,
) -> list[dict[Any, Any]]:
    """Get pilots by their stamp.

    If `allow_missing` is set to False, if a pilot is missing, PilotNotFoundError will be raised.
    """
    if parameters:
        parameters.append("PilotStamp")

    _, pilots = await pilot_db.search_pilots(
        parameters=parameters,
        search=[
            VectorSearchSpec(
                parameter="PilotStamp",
                operator=VectorSearchOperator.IN,
                values=pilot_stamps,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=MAX_PER_PAGE,
    )

    # allow_missing is set as True by default to mark explicitly when we allow or not
    if not allow_missing:
        # Custom handling, to see which pilot_stamp does not exist (if so, say which one)
        found_keys = {row["PilotStamp"] for row in pilots}
        missing = set(pilot_stamps) - found_keys

        if missing:
            raise PilotNotFoundError(
                data={"pilot_stamp": str(missing)},
                detail=str(missing),
                non_existing_pilots=missing,
            )

    return pilots


async def get_pilot_ids_by_stamps(
    pilot_db: PilotAgentsDB, pilot_stamps: list[str]
) -> list[int]:
    pilots = await get_pilots_by_stamp(
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
        parameters=["PilotID"],
        allow_missing=False,
    )

    return [pilot["PilotID"] for pilot in pilots]


async def get_pilot_jobs_ids_by_pilot_id(
    pilot_db: PilotAgentsDB, pilot_id: int
) -> list[int]:
    _, jobs = await pilot_db.search_pilot_to_job_mapping(
        parameters=["JobID"],
        search=[
            ScalarSearchSpec(
                parameter="PilotID",
                operator=ScalarSearchOperator.EQUAL,
                value=pilot_id,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=MAX_PER_PAGE,
    )

    return [job["JobID"] for job in jobs]


async def get_pilot_ids_by_job_id(pilot_db: PilotAgentsDB, job_id: int) -> list[int]:
    _, pilots = await pilot_db.search_pilot_to_job_mapping(
        parameters=["PilotID"],
        search=[
            ScalarSearchSpec(
                parameter="JobID",
                operator=ScalarSearchOperator.EQUAL,
                value=job_id,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=MAX_PER_PAGE,
    )

    return [pilot["PilotID"] for pilot in pilots]
