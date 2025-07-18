"""File dedicated to logic for pilot only resources (logs, jobs, etc.)."""

from __future__ import annotations

from diracx.core.exceptions import PilotNotFoundError
from diracx.core.models import LogLine
from diracx.db.os.pilot_logs import PilotLogsDB
from diracx.db.sql.pilots.db import PilotAgentsDB

from .query import get_pilot_ids_by_stamps


async def send_message(
    lines: list[LogLine],
    pilot_logs_db: PilotLogsDB,
    pilot_db: PilotAgentsDB,
    vo: str,
    pilot_stamp: str,
):
    try:
        pilot_ids = await get_pilot_ids_by_stamps(
            pilot_db=pilot_db, pilot_stamps=[pilot_stamp]
        )
        pilot_id = pilot_ids[0]  # Semantic
    except PilotNotFoundError:
        # If a pilot is not found, then we still store the data (to not lost it)
        # We log it as it's not supposed to happen
        # If we arrive here, the pilot as been deleted but is still "alive"
        pilot_id = -1  # To detect

    docs = []
    for line in lines:
        docs.append(
            {
                "PilotStamp": pilot_stamp,
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
