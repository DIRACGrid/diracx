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
        split = int(int(doc_id) // 1e6)
        # We split docs into smaller one (grouped by 1 million pilot)
        # Ex: pilot_logs_dteam_1030m
        return f"{self.index_prefix}_{vo.lower()}_{split}m"
