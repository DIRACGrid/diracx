from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import select

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


@router.post("/")
async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
):
    logger.warning(f"Message received '{data}'")
    await check_permissions(action=ActionType.CREATE, pilot_db=pilot_logs_db)

    pilot_id = 0  # need to get pilot id from pilot_stamp (via PilotAgentsDB)
    piloAgentsDB = BaseSQLDB.available_implementations("PilotAgentsDB")[0]
    url = BaseSQLDB.available_urls()["PilotAgentsDB"]
    db = piloAgentsDB(url)

    async with db.engine_context():
        async with db:
            stmt = select(
                (PilotAgents.PilotID).where(PilotAgents.PilotStamp == data.pilot_stamp)
            )
            pilot_id = (await db.conn.execute(stmt)).scalar_one()

    docs = []
    for line in data.lines:
        docs.append(
            {
                "PilotStamp": data.pilot_stamp,
                "VO": data.vo,
                "LineNumber": line.line_no,
                "Message": line.line,
            }
        )
    await pilot_logs_db.bulk_insert(pilot_logs_db.index_name(pilot_id), docs)
    return data
