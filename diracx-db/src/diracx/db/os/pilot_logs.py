from __future__ import annotations

from diracx.db.os.utils import BaseOSDB


class PilotLogsDB(BaseOSDB):
    fields = {
        "PilotStamp": {"type": "keyword"},
        "LineNumber": {"type": "long"},
        "Message": {"type": "text"},
        "VO": {"type": "keyword"},
        "timestamp": {"type": "date"},
    }
    index_prefix = "pilot_logs"

    def index_name(self, doc_id: int) -> str:
        # TODO decide how to define the index name
        return f"{self.index_prefix}_0"
