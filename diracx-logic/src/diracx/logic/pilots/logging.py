from __future__ import annotations

import logging

from diracx.core.models import LogMessage, ScalarSearchOperator, ScalarSearchSpec
from diracx.db.os.pilot_logs import PilotLogsDB, search_message
from diracx.db.sql.pilot_agents.db import PilotAgentsDB

logger = logging.getLogger(__name__)


async def send_message(
    data: LogMessage,
    pilot_logs_db: PilotLogsDB,
    pilot_agents_db: PilotAgentsDB,
) -> int:

    # get the pilot ID corresponding to a given pilot stamp, expecting exactly one row:
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
        raise Exception(f"Number of rows !=1 {total}")

    pilot_id, vo, submission_time = (
        result[0]["PilotID"],
        result[0]["VO"],
        result[0]["SubmissionTime"],
    )
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
    # bulk insert pilot logs to OpenSearch DB:
    await pilot_logs_db.bulk_insert(pilot_logs_db.index_name(pilot_id), docs)
    return pilot_id


async def get_logs(
    pilot_id: int,
    db: PilotLogsDB,
) -> list[dict]:

    search_params = [{"parameter": "PilotID", "operator": "eq", "value": pilot_id}]

    result = await search_message(db, search_params)

    if not result:
        return [{"Message": f"No logs for pilot ID = {pilot_id}"}]
    return result
