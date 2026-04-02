"""Integration tests for the callback system."""

from __future__ import annotations

from unittest.mock import AsyncMock

import msgpack

from diracx.tasks.plumbing.callbacks import fire_callback, on_child_complete

from .conftest import get_enqueued_messages


async def test_on_child_complete_returns_false_when_remaining():
    """on_child_complete should return False when siblings remain."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock()
    mock_redis.decr = AsyncMock(return_value=2)

    result = await on_child_complete(mock_redis, "group1", "child1", "result1")
    assert result is False


async def test_on_child_complete_returns_true_when_last():
    """on_child_complete should return True when this is the last child."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock()
    mock_redis.decr = AsyncMock(return_value=0)

    result = await on_child_complete(mock_redis, "group1", "child3", "result3")
    assert result is True


async def test_on_child_complete_stores_result():
    """on_child_complete should store the child's result in Redis."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock()
    mock_redis.decr = AsyncMock(return_value=1)

    await on_child_complete(mock_redis, "grp", "c1", {"value": 42})

    # Should have stored the result
    mock_redis.set.assert_called_once()
    call_args = mock_redis.set.call_args
    assert "diracx:groups:grp:results:c1" in call_args[0]


async def test_fire_callback_kicks_task(broker):
    """fire_callback should deserialize callback data and enqueue to broker."""
    callback_data = msgpack.packb(
        {
            "task_class": "test.module:CallbackTask",
            "args": ["arg1", "arg2"],
        },
        datetime=True,
    )

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=callback_data)

    await fire_callback(mock_redis, "group123", broker)

    messages = await get_enqueued_messages(broker)
    assert len(messages) == 1
    assert messages[0].task_name == "test.module:CallbackTask"
    assert messages[0].labels["callback_group_id"] == "group123"


async def test_fire_callback_logs_missing_data(broker):
    """fire_callback should handle missing callback data gracefully."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)

    # Should not raise
    await fire_callback(mock_redis, "missing_group", broker)

    # No message enqueued
    messages = await get_enqueued_messages(broker)
    assert len(messages) == 0
