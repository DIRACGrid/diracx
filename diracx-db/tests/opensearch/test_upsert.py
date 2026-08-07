from __future__ import annotations

import pytest

from diracx.core.exceptions import DocumentUpsertError
from diracx.testing.osdb import DummyOSDB


async def test_upsert_valid_document(dummy_opensearch_db: DummyOSDB):
    """Sanity check that a well-formed document can be upserted."""
    await dummy_opensearch_db.upsert("dummyvo", 1, {"IntField": 1234})
    await dummy_opensearch_db.client.indices.refresh(
        index=f"{dummy_opensearch_db.index_prefix}*"
    )
    results = await dummy_opensearch_db.search(
        None, [{"parameter": "IntField", "operator": "eq", "value": "1234"}], []
    )
    assert len(results) == 1


async def test_upsert_unparsable_document_raises(dummy_opensearch_db: DummyOSDB):
    """NaN survives Python JSON serialization but OpenSearch rejects it.

    This must surface as a DocumentUpsertError rather than an unhandled
    RequestError, and the offending document must be logged.
    """
    with pytest.raises(DocumentUpsertError, match="Failed to upsert document"):
        await dummy_opensearch_db.upsert("dummyvo", 2, {"IntField": float("nan")})
