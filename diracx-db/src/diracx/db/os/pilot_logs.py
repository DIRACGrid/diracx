from __future__ import annotations

from diracx.db.os.utils import BaseOSDB


class PilotLogsDB(BaseOSDB):
    fields = {
        "PilotStamp": {"type": "keyword"},
        "PilotID": {"type": "long"},
        "Severity": {"type": "keyword"},
        "Message": {"type": "text"},
        "VO": {"type": "keyword"},
        "TimeStamp": {"type": "date_nanos"},
        "Scope": {"type": "keyword"},
    }
    index_prefix = "pilot_logs"

    def index_name(self, vo: str, doc_id: int) -> str:
        # TODO decide how to define the index name
        # use pilot ID
        return f"{self.index_prefix}_{doc_id}"
