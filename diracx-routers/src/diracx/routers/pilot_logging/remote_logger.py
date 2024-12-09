from __future__ import annotations

import datetime

from fastapi import HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound

from diracx.core.exceptions import InvalidQueryError
from diracx.core.properties import OPERATOR, SERVICE_ADMINISTRATOR
from diracx.db.sql.pilot_agents.schema import PilotAgents
from diracx.db.sql.utils import BaseSQLDB

from ..dependencies import PilotLogsDB
from ..fastapi_classes import DiracxRouter
from ..pilot_logging import logger
from ..utils.users import AuthorizedUserInfo
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
    min: str | None = None  # expects a string in ISO 8601 ("%Y-%m-%dT%H:%M:%S.%f%z")
    max: str | None = None  # expects a string in ISO 8601 ("%Y-%m-%dT%H:%M:%S.%f%z")


@router.post("/")
async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
) -> int:

    logger.warning(f"Message received '{data}'")
    user_info = await check_permissions(
        action=ActionType.CREATE, pilot_db=pilot_logs_db
    )
    pilot_id = 0  # need to get pilot id from pilot_stamp (via PilotAgentsDB)
    # also add a timestamp to be able to select and delete logs based on pilot creation dates, even if corresponding
    # pilots have been already deleted from PilotAgentsDB (so the logs can live longer than pilots).
    submission_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    piloAgentsDB = BaseSQLDB.available_implementations("PilotAgentsDB")[0]
    url = BaseSQLDB.available_urls()["PilotAgentsDB"]
    db = piloAgentsDB(url)

    try:
        async with db.engine_context():
            async with db:
                stmt = select(PilotAgents.PilotID, PilotAgents.SubmissionTime).where(
                    PilotAgents.PilotStamp == data.pilot_stamp
                )
                pilot_id, submission_time = (await db.conn.execute(stmt)).one()
    except NoResultFound as exc:
        logger.error(
            f"Cannot determine PilotID for requested PilotStamp: {data.pilot_stamp}, Error: {exc}."
        )
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    docs = []
    for line in data.lines:
        docs.append(
            {
                "PilotStamp": data.pilot_stamp,
                "PilotID": pilot_id,
                "SubmissionTime": submission_time,
                "VO": user_info.vo,
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
) -> list[dict]:

    logger.warning(f"Retrieving logs for pilot ID '{pilot_id}'")
    user_info = await check_permissions(action=ActionType.QUERY, pilot_db=db)

    # here, users with privileged properties will see logs from all VOs. Is it what we want ?
    search_params = [{"parameter": "PilotID", "operator": "eq", "value": pilot_id}]
    if _non_privileged(user_info):
        search_params.append(
            {"parameter": "VO", "operator": "eq", "value": user_info.vo}
        )
    result = await db.search(
        ["Message"],
        search_params,
        [{"parameter": "LineNumber", "direction": "asc"}],
    )
    if not result:
        return [{"Message": f"No logs for pilot ID = {pilot_id}"}]
    return result


@router.delete("/logs")
async def delete(
    pilot_id: int,
    data: DateRange,
    db: PilotLogsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
) -> str:
    """Delete either logs for a specific PilotID or a creation date range.
    Non-privileged users can only delete log files within their own VO.
    """
    message = "no-op"
    user_info = await check_permissions(action=ActionType.DELETE, pilot_db=db)
    non_privil_params = {"parameter": "VO", "operator": "eq", "value": user_info.vo}

    # id pilot_id is provided we ignore data.min and data.max
    if data.min and data.max and not pilot_id:
        raise InvalidQueryError(
            "This query requires a range operator definition in DiracX"
        )

    if pilot_id:
        search_params = [{"parameter": "PilotID", "operator": "eq", "value": pilot_id}]
        if _non_privileged(user_info):
            search_params.append(non_privil_params)
        await db.delete(search_params)
        message = f"Logs for pilot ID '{pilot_id}' successfully deleted"

    elif data.min:
        logger.warning(f"Deleting logs for pilots with submission data >='{data.min}'")
        search_params = [
            {"parameter": "SubmissionTime", "operator": "gt", "value": data.min}
        ]
        if _non_privileged(user_info):
            search_params.append(non_privil_params)
        await db.delete(search_params)
        message = f"Logs for for pilots with submission data >='{data.min}' successfully deleted"

    return message


def _non_privileged(user_info: AuthorizedUserInfo):
    return (
        SERVICE_ADMINISTRATOR not in user_info.properties
        and OPERATOR not in user_info.properties
    )
