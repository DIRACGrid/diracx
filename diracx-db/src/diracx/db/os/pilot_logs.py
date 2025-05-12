from __future__ import annotations

from diracx.db.os.utils import BaseOSDB


class PilotLogsDB(BaseOSDB):
    fields = {
        "PilotStamp": {"type": "keyword"},
        "PilotID": {"type": "long"},
        "SubmissionTime": {"type": "date"},
        "LineNumber": {"type": "long"},
        "Message": {"type": "text"},
        "VO": {"type": "keyword"},
        "timestamp": {"type": "date"},
    }
    index_prefix = "pilot_logs"

    def index_name(self, doc_id: int) -> str:
        # TODO decide how to define the index name
        # use pilot ID
        return f"{self.index_prefix}_{doc_id // 1e6:.0f}"


async def search_message(db: PilotLogsDB, search_params: list[dict]):

    return await db.search(
        ["Message"],
        search_params,
        [{"parameter": "LineNumber", "direction": "asc"}],
    )
