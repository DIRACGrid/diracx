"""Tests for the Redis stream broker and 9-stream routing."""

from __future__ import annotations

from diracx.tasks.plumbing.broker.redis_streams import (
    ALL_STREAM_NAMES,
    stream_name_for,
)
from diracx.tasks.plumbing.enums import Priority, Size


def test_stream_name_for():
    assert (
        stream_name_for(Priority.REALTIME, Size.SMALL) == "diracx:tasks:realtime:small"
    )
    assert stream_name_for(Priority.NORMAL, Size.MEDIUM) == "diracx:tasks:normal:medium"
    assert (
        stream_name_for(Priority.BACKGROUND, Size.LARGE)
        == "diracx:tasks:background:large"
    )


def test_all_stream_names():
    assert len(ALL_STREAM_NAMES) == 9
    assert "diracx:tasks:realtime:small" in ALL_STREAM_NAMES
    assert "diracx:tasks:normal:medium" in ALL_STREAM_NAMES
    assert "diracx:tasks:background:large" in ALL_STREAM_NAMES
