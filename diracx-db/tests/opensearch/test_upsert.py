from __future__ import annotations

import logging

import pytest
from opensearchpy.exceptions import RequestError

from diracx.core.exceptions import DocumentUpsertError
from diracx.testing.osdb import DummyOSDB


class _RejectingClient:
    """Minimal stand-in for AsyncOpenSearch which rejects every update."""

    async def update(self, **kwargs):
        raise RequestError(
            400,
            "mapper_parsing_exception",
            {
                "error": {
                    "reason": "failed to parse field [IntField] of type [long] in "
                    "document with id '1234'. Preview of field's value: 'a value'"
                }
            },
        )


async def test_upsert_rejected_document(caplog):
    """A document the backend refuses to index raises a DocumentUpsertError.

    The reason is logged for the administrator, not returned to the client, and
    the client-supplied values are only logged at debug level.
    """
    db = DummyOSDB({"hosts": "http://localhost:9200"})
    db._client = _RejectingClient()

    with caplog.at_level(logging.ERROR, logger="diracx.db.os.utils"):
        with pytest.raises(DocumentUpsertError) as exc_info:
            await db.upsert("dummyvo", 1234, {"IntField": 1, "TextField": "a value"})

    assert "mapper_parsing_exception" not in str(exc_info.value)
    assert "mapper_parsing_exception" in caplog.text
    assert "IntField" in caplog.text
    assert "a value" not in caplog.text


async def test_bulk_upsert(dummy_opensearch_db: DummyOSDB, caplog):
    """Stored documents can be searched, rejected ones are returned and logged."""
    documents = [
        ("dummyvo", 1, {"IntField": 1, "KeywordField1": "a"}),
        ("dummyvo", 2, {"IntField": "a value"}),
    ]
    with caplog.at_level(logging.ERROR, logger="diracx.db.os.utils"):
        success, errors = await dummy_opensearch_db.bulk_upsert(documents)

    assert success == 1
    assert [error["update"]["_id"] for error in errors] == ["2"]
    assert "mapper_parsing_exception" in caplog.text
    assert "IntField" in caplog.text
    assert "a value" not in caplog.text

    # Upserting again merges into the existing document
    await dummy_opensearch_db.bulk_upsert([("dummyvo", 1, {"KeywordField2": "b"})])
    await dummy_opensearch_db.client.indices.refresh(
        index=f"{dummy_opensearch_db.index_prefix}*"
    )
    results = await dummy_opensearch_db.search(None, [], [])
    assert results == [{"IntField": 1, "KeywordField1": "a", "KeywordField2": "b"}]
