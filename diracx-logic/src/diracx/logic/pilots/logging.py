from __future__ import annotations

import logging

from diracx.core.exceptions import PilotNotFoundError
from diracx.core.models import (
    LogMessage,
    SearchParams,
    SortDirection,
)
from diracx.db.os.pilot_logs import PilotLogsDB
from diracx.db.sql.pilot_agents.db import PilotAgentsDB

logger = logging.getLogger(__name__)


MAX_PER_PAGE = 10000


async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
    vo: str,
):
    try:
        pilot_ids = await pilot_agents_db.get_pilot_ids_by_stamps([data.pilot_stamp])
        pilot_id = pilot_ids[0]  # Semantic
    except PilotNotFoundError:
        # If a pilot is not found, then we still store the data (to not lost it)
        # We log it as it's not supposed to happen
        pilot_id = -1  # To detect

    docs = []
    for line in data.lines:
        docs.append(
            {
                "PilotStamp": data.pilot_stamp,
                "PilotID": pilot_id,
                "VO": vo,
                "Severity": line.severity,
                "Message": line.message,
                "TimeStamp": line.timestamp,
                "Scope": line.scope,
            }
        )
    # bulk insert pilot logs to OpenSearch DB:
    await pilot_logs_db.bulk_insert(pilot_logs_db.index_name(vo, pilot_id), docs)


async def search_logs(
    vo: str,
    body: SearchParams | None,
    per_page: int,
    page: int,
    pilot_logs_db: PilotLogsDB,
) -> list[dict]:
    """Retrieve logs from OpenSearch for a given PilotStamp."""
    # Apply a limit to per_page to prevent abuse of the API
    if per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

    if body is None:
        body = SearchParams()

    search = body.search
    parameters = body.parameters
    sorts = body.sort

    # Add the vo to make sure that we filter for pilots we can see
    # TODO: Test it
    search = search + [
        {
            "parameter": "VO",
            "operator": "eq",
            "value": vo,
        }
    ]

    if not sorts:
        sorts = [{"parameter": "TimeStamp", "direction": SortDirection("asc")}]

    return await pilot_logs_db.search(
        parameters=parameters, search=search, sorts=sorts, per_page=per_page, page=page
    )
