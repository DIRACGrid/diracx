from __future__ import annotations

import datetime

from pydantic import BaseModel
from sqlalchemy import select

from diracx.core.exceptions import InvalidQueryError
from diracx.db.sql.pilot_agents.schema import PilotAgents
from diracx.db.sql.utils import BaseSQLDB

from ..dependencies import PilotLogsDB
from ..fastapi_classes import DiracxRouter
from ..pilot_logging import logger
from .access_policies import ActionType, CheckPilotLogsPolicyCallable

router = DiracxRouter()


class LogLine(BaseModel):
    line_no: int
    line: str


class LogMessage(BaseModel):
    pilot_stamp: str
    lines: list[LogLine]
    vo: str


class DateRange(BaseModel):
    pilot_id: int | None = None
    min: str | None = None  # expects a string in ISO 8601 ("%Y-%m-%dT%H:%M:%S.%f%z")
    max: str | None = None  # expects a string in ISO 8601 ("%Y-%m-%dT%H:%M:%S.%f%z")


@router.post("/")
async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
):
    logger.warning(f"Message received '{data}'")
    await check_permissions(action=ActionType.CREATE, pilot_db=pilot_logs_db)

    pilot_id = 0  # need to get pilot id from pilot_stamp (via PilotAgentsDB)
    # also add a timestamp to be able to select and delete logs based on pilot creation dates, even if corresponding
    # pilots have been already deleted from PilotAgentsDB (so the logs can live longer than pilots).
    submission_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    piloAgentsDB = BaseSQLDB.available_implementations("PilotAgentsDB")[0]
    url = BaseSQLDB.available_urls()["PilotAgentsDB"]
    db = piloAgentsDB(url)

    async with db.engine_context():
        async with db:
            stmt = select(PilotAgents.PilotID, PilotAgents.SubmissionTime).where(
                PilotAgents.PilotStamp == data.pilot_stamp
            )
            pilot_id, submission_time = (await db.conn.execute(stmt)).one()

    docs = []
    for line in data.lines:
        docs.append(
            {
                "PilotStamp": data.pilot_stamp,
                "PilotID": pilot_id,
                "SubmissionTime": submission_time,
                "VO": data.vo,
                "LineNumber": line.line_no,
                "Message": line.line,
            }
        )
    await pilot_logs_db.bulk_insert(pilot_logs_db.index_name(pilot_id), docs)
    return pilot_id


@router.get("/logs")
async def get_logs(
    pilot_id: int,
    db: PilotLogsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
):
    logger.warning(f"Retrieving logs for pilot ID '{pilot_id}'")
    await check_permissions(action=ActionType.QUERY, pilot_db=db)

    result = await db.search(
        ["Message"],
        [{"parameter": "PilotID", "operator": "eq", "value": pilot_id}],
        [{"parameter": "LineNumber", "direction": "asc"}],
    )
    if not result:
        return [f"No logs for pilot ID = {pilot_id}"]
    return result


@router.delete("/logs")
async def delete(
    data: DateRange,
    db: PilotLogsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
) -> str:
    """Delete either logs for a specific PilotID or a creation date range."""
    await check_permissions(action=ActionType.DELETE, pilot_db=db)
    if data.pilot_id:
        await db.delete(
            [{"parameter": "PilotID", "operator": "eq", "value": data.pilot_id}]
        )
        return f"Logs for pilot ID '{data.pilot_id}' successfully deleted"
    if data.min and not data.max:
        logger.warning(f"Deleting logs for pilots with submission data >='{data.min}'")
        await db.delete(
            [{"parameter": "SubmissionTime", "operator": "gt", "value": data.min}]
        )
        return f"Logs for for pilots with submission data >='{data.min}' successfully deleted"
    if data.min and data.max:
        raise InvalidQueryError(
            "This query requires a range operater definition in DiracX"
        )
    return "no-op"
