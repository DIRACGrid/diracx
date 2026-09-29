from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from diracx.db.os.utils import BaseOSDB


class JobParametersDB(BaseOSDB):
    fields = {
        "JobID": {"type": "long"},
        "timestamp": {"type": "date"},
        "PilotAgent": {"type": "keyword"},
        "Pilot_Reference": {"type": "keyword"},
        "CPUNormalizationFactor": {"type": "long"},
        "NormCPUTime(s)": {"type": "long"},
        "Memory(MB)": {"type": "long"},
        "LocalAccount": {"type": "keyword"},
        "TotalCPUTime(s)": {"type": "long"},
        "PayloadPID": {"type": "long"},
        "HostName": {"type": "text"},
        "GridCE": {"type": "keyword"},
        "CEQueue": {"type": "keyword"},
        "BatchSystem": {"type": "keyword"},
        "ModelName": {"type": "keyword"},
    }
    # TODO: Does this need to be configurable?
    index_prefix = "job_parameters"

    def index_name(self, vo, doc_id: int) -> str:
        split = int(int(doc_id) // 1e6)
        # The index name must be lowercase or opensearchpy will throw.
        return f"{self.index_prefix}_{vo.lower()}_{split}m"

    @staticmethod
    def _with_metadata(doc_id: int, document: dict[str, Any], timestamp: int):
        return {"JobID": doc_id, "timestamp": timestamp, **document}

    def upsert(self, vo, doc_id, document):
        timestamp = int(datetime.now(tz=UTC).timestamp() * 1000)
        document = self._with_metadata(doc_id, document, timestamp)
        return super().upsert(vo, doc_id, document)

    async def bulk_upsert(
        self,
        documents: Iterable[tuple[str, int, dict[str, Any]]],
    ) -> tuple[int, list[Any]]:
        """bulk_upsert API implementation."""
        timestamp = int(datetime.now(tz=UTC).timestamp() * 1000)
        return await super().bulk_upsert(
            (vo, doc_id, self._with_metadata(doc_id, document, timestamp))
            for vo, doc_id, document in documents
        )
