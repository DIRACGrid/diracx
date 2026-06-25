"""OS-backed job parameter storage.

This module provides the :class:`JobParametersDB` class, which stores and
upserts job parameter documents into an object store / OpenSearch-backed
index. Documents are sharded by job ID and include a timestamp when written.
"""

from __future__ import annotations

from datetime import UTC, datetime

from diracx.db.os.utils import BaseOSDB


class JobParametersDB(BaseOSDB):
    """OS-backed storage accessor for job parameter documents.

    This class provides helpers to store and upsert arbitrary job-related
    parameters in an object-store / OpenSearch-backed index. Documents are
    sharded by job ID using ``index_name`` and include a timestamp field
    added by ``upsert``.
    """

    fields = {
        "JobID": {"type": "long"},
        "timestamp": {"type": "date"},
        "PilotAgent": {"type": "keyword"},
        "Pilot_Reference": {"type": "keyword"},
        "JobGroup": {"type": "keyword"},
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
        "Status": {"type": "keyword"},
        "JobType": {"type": "keyword"},
    }
    # TODO: Does this need to be configurable?
    index_prefix = "job_parameters"

    def index_name(self, vo, doc_id: int) -> str:
        """Return the index name for a job document.

        The index is sharded using million-sized buckets based on ``doc_id``
        (job ID). For example a job with ID 1234567 is placed into the
        ``1m`` shard. The returned index name is lowercased to satisfy the
        OpenSearch naming requirements.

        Args:
            vo (str): VO identifier used as part of the index prefix.
            doc_id (int): Numeric job ID.

        Returns:
            str: A lowercased index name like
                ``job_parameters_<vo>_<N>m`` where ``N`` is the million-shard.
        """
        split = int(int(doc_id) // 1e6)
        # The index name must be lowercase or opensearchpy will throw.
        return f"{self.index_prefix}_{vo.lower()}_{split}m"

    def upsert(self, vo, doc_id, document):
        """Upsert a parameter document for a job.

        This method normalizes the supplied ``document`` by ensuring the
        required ``JobID`` and ``timestamp`` fields are present, then
        delegates to :meth:`BaseOSDB.upsert` for persistence.

        Args:
            vo (str): VO identifier used to determine the index name.
            doc_id (int): Job ID used as the document identifier.
            document (dict): Arbitrary parameter key/value pairs to store.

        Returns:
            Any: The underlying storage driver's upsert result.
        """
        document = {
            "JobID": doc_id,
            "timestamp": int(datetime.now(tz=UTC).timestamp() * 1000),
            **document,
        }
        return super().upsert(vo, doc_id, document)
