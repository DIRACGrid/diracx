from __future__ import annotations

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
    index_prefix = "mysetup_elasticjobparameters_index_"

    def index_name(self, doc_id: int) -> str:
        # TODO: Remove setup and replace "123.0m" with "120m"?
        return f"{self.index_prefix}_{doc_id // 1e6:.1f}m"
