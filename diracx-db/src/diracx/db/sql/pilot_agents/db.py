from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, insert, select

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import (
    SearchSpec,
    SortSpec,
)

from ..utils import BaseSQLDB, apply_search_filters, apply_sort_constraints, get_columns
from .schema import PilotAgents, PilotAgentsDBBase


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    async def add_pilot_references(
        self,
        pilot_ref: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_stamps: dict | None = None,
    ) -> None:

        if pilot_stamps is None:
            pilot_stamps = {}

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {
                "PilotJobReference": ref,
                "VO": vo,
                "GridType": grid_type,
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": "Submitted",
                "PilotStamp": pilot_stamps.get(ref, ""),
            }
            for ref in pilot_ref
        ]

        # Insert multiple rows in a single execute call
        stmt = insert(PilotAgents).values(values)
        await self.conn.execute(stmt)
        return

    async def search(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        # Find which columns to select
        columns = get_columns(PilotAgents.__table__, parameters)

        stmt = select(*columns)

        stmt = apply_search_filters(
            PilotAgents.__table__.columns.__getitem__, stmt, search
        )
        stmt = apply_sort_constraints(
            PilotAgents.__table__.columns.__getitem__, stmt, sorts
        )

        if distinct:
            stmt = stmt.distinct()

        # Calculate total count before applying pagination
        total_count_subquery = stmt.alias()
        total_count_stmt = select(func.count()).select_from(total_count_subquery)
        total = (await self.conn.execute(total_count_stmt)).scalar_one()

        # Apply pagination
        if page is not None:
            if page < 1:
                raise InvalidQueryError("Page must be a positive integer")
            if per_page < 1:
                raise InvalidQueryError("Per page must be a positive integer")
            stmt = stmt.offset((page - 1) * per_page).limit(per_page)

        # Execute the query
        return total, [
            dict(row._mapping) async for row in (await self.conn.stream(stmt))
        ]
