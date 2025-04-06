from __future__ import annotations

from DIRAC.Core.Utilities import TimeUtilities

from diracx.db.os.utils import BaseOSDB


class JobParametersDB(BaseOSDB):
    fields = {
        "JobID": {"type": "long"},
        "timestamp": {"type": "date"},
        "CPUNormalizationFactor": {"type": "long"},
        "NormCPUTime(s)": {"type": "long"},
        "Memory(kB)": {"type": "long"},
        "TotalCPUTime(s)": {"type": "long"},
        "MemoryUsed(kb)": {"type": "long"},
        "HostName": {"type": "keyword"},
        "GridCE": {"type": "keyword"},
        "ModelName": {"type": "keyword"},
        "Status": {"type": "keyword"},
        "JobType": {"type": "keyword"},
    }
    # TODO: Does this need to be configurable?
    index_prefix = "job_parameters"

    def index_name(self, vo, doc_id: int) -> str:
        split = int(int(doc_id) // 1e6)
        return f"{self.index_prefix}_{vo}_{split}m"

    def upsert(self, vo, doc_id, document):
        document = {
            "JobID": doc_id,
            "timestamp": TimeUtilities.toEpochMilliSeconds(),
            **document,
        }
        return super().upsert(vo, doc_id, document)
