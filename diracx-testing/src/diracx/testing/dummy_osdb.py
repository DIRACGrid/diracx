from __future__ import annotations

__all__ = []

import secrets

from diracx.db.os.utils import BaseOSDB


class DummyOSDB(BaseOSDB):
    """Example DiracX OpenSearch database class for testing.

    A new random prefix is created each time the class is defined to ensure
    test runs are independent of each other.
    """

    fields = {
        "DateField": {"type": "date"},
        "IntField": {"type": "long"},
        "KeywordField0": {"type": "keyword"},
        "KeywordField1": {"type": "keyword"},
        "KeywordField2": {"type": "keyword"},
        "TextField": {"type": "text"},
    }

    index_prefix = "hastobedefinedasabstractinthebase"

    def __init__(self, *args, **kwargs):
        # Randomize the index prefix to ensure tests are independent
        self.index_prefix = f"dummy_{secrets.token_hex(8)}"
        # Since we mangle with the index prefix,
        # and that the global_prefix is managed in the base class
        # Keep the super call after the index_prefix overwrite
        super().__init__(*args, **kwargs)

    def index_name(self, vo: str, doc_id: int) -> str:
        return f"{self.index_prefix}-{doc_id // 1e6:.0f}m"
