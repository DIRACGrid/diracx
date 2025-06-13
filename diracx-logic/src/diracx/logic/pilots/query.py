from __future__ import annotations

from typing import Any

from diracx.core.models import ScalarSearchOperator, SearchParams
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
        {"parameter": "VO", "operator": ScalarSearchOperator.EQUAL, "value": user_vo}
    )

    total, pilots = await pilot_db.search(
        body.parameters,
        body.search,
        body.sort,
        distinct=body.distinct,
        page=page,
        per_page=per_page,
    )

    return total, pilots
