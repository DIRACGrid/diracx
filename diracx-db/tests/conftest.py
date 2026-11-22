from __future__ import annotations

import pytest

from diracx.db.sql.utils import chunks


@pytest.fixture
def small_chunks(monkeypatch):
    """Split IN lists into chunks of two, so a few rows cross chunk boundaries.

    Fails the test if no call was split into more than one chunk, which
    would mean the size was bound before the test could lower it.
    """
    monkeypatch.setattr(chunks, "IN_CHUNK_SIZE", 2)
    split_calls = 0
    original = chunks._chunks

    def counting(values, chunk_size):
        nonlocal split_calls
        produced = list(original(values, chunk_size))
        split_calls += len(produced) > 1
        yield from produced

    monkeypatch.setattr(chunks, "_chunks", counting)
    yield
    assert split_calls, "no call was split into more than one chunk"
