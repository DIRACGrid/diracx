"""Tests for the Redis stream broker and 9-stream routing."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from diracx.tasks.plumbing.broker.redis_streams import (
    ALL_STREAM_NAMES,
    RedisStreamBroker,
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


async def test_renew_generator_calls_xclaim() -> None:
    broker = RedisStreamBroker("redis://example.invalid")

    redis_cm = AsyncMock()
    redis = AsyncMock()
    redis_cm.__aenter__.return_value = redis
    redis_cm.__aexit__.return_value = False

    with patch(
        "diracx.tasks.plumbing.broker.redis_streams.Redis", return_value=redis_cm
    ):
        renew = broker._renew_generator(
            msg_id="1234-0",
            queue_name="diracx:tasks:normal:medium",
        )
        await renew()

    redis.xclaim.assert_awaited_once_with(
        "diracx:tasks:normal:medium",
        broker.consumer_group_name,
        broker.consumer_name,
        min_idle_time=0,
        message_ids=["1234-0"],
    )
