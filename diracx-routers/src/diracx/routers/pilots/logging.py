from __future__ import annotations

import logging

from fastapi import HTTPException, status
from pydantic import BaseModel

from diracx.core.models import ScalarSearchOperator, ScalarSearchSpec

from ..access_policies import open_access
from ..dependencies import PilotAgentsDB, PilotLogsDB
from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, CheckPilotLogsPolicyCallable

logger = logging.getLogger(__name__)
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


@open_access
@router.post("/")
async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
    # user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> int:

    # expecting exactly one row:
    search_params = ScalarSearchSpec(
        parameter="PilotStamp",
        operator=ScalarSearchOperator.EQUAL,
        value=data.pilot_stamp,
    )

    total, result = await pilot_agents_db.search(
        ["PilotID", "VO", "SubmissionTime"], [search_params], []
    )
    if total != 1:
        logger.error(
            "Cannot determine PilotID for requested PilotStamp: %r, (%d candidates)",
            data.pilot_stamp,
            total,
        )
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, detail=f"Number of rows !=1: {total}"
        )
    pilot_id, vo, submission_time = (
        result[0]["PilotID"],
        result[0]["VO"],
        result[0]["SubmissionTime"],
    )

    # await check_permissions(action=ActionType.CREATE, pilot_agent_db, pilot_id),

    docs = []
    for line in data.lines:
        docs.append(
            {
                "PilotStamp": data.pilot_stamp,
                "PilotID": pilot_id,
                "SubmissionTime": submission_time,
                "VO": vo,
                "LineNumber": line.line_no,
                "Message": line.line,
            }
        )
    await pilot_logs_db.bulk_insert(pilot_logs_db.index_name(pilot_id), docs)
    """
    search_params = [{"parameter": "PilotID", "operator": "eq", "value": pilot_id}]

    result = await pilot_logs_db.search(
        ["Message"],
        search_params,
        [{"parameter": "LineNumber", "direction": "asc"}],
    )
    """
    return pilot_id


@router.get("/logs")
async def get_logs(
    pilot_id: int,
    db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotLogsPolicyCallable,
) -> list[dict]:

    logger.debug("Retrieving logs for pilot ID %d", pilot_id)
    # users will only see logs from their own VO if enforced by a policy:
    await check_permissions(
        action=ActionType.QUERY, pilot_agents_db=pilot_agents_db, pilot_id=pilot_id
    )

    search_params = [{"parameter": "PilotID", "operator": "eq", "value": pilot_id}]

    result = await db.search(
        ["Message"],
        search_params,
        [{"parameter": "LineNumber", "direction": "asc"}],
    )
    if not result:
        return [{"Message": f"No logs for pilot ID = {pilot_id}"}]
    return result
