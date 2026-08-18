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
            "x_content_parse_exception",
            {"error": {"reason": "[1:54] [UpdateRequest] failed to parse field [doc]"}},
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

    assert "x_content_parse_exception" not in str(exc_info.value)
    assert "x_content_parse_exception" in caplog.text
    assert "IntField" in caplog.text
    assert "a value" not in caplog.text
