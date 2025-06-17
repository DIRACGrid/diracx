"""File dedicated to logic for pilot only resources (logs, jobs, etc.)."""

from __future__ import annotations

import logging

from diracx.core.exceptions import PilotNotFoundError
from diracx.core.models import (
    LogMessage,
)
from diracx.db.os.pilot_logs import PilotLogsDB
from diracx.db.sql.pilots.db import PilotAgentsDB

from .query import get_pilot_ids_by_stamps

logger = logging.getLogger(__name__)


MAX_PER_PAGE = 10000


async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    pilot_db: PilotAgentsDB,
    vo: str,
):
    try:
        pilot_ids = await get_pilot_ids_by_stamps(
            pilot_db=pilot_db, pilot_stamps=[data.pilot_stamp]
        )
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
