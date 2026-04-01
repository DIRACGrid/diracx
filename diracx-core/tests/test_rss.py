from __future__ import annotations

import pytest

from diracx.core.models.rss import (
    AllowedStatus,
    BannedStatus,
    map_status,
)


@pytest.mark.parametrize("status", ["Active", "Degraded"])
async def test_map_status_allowed(status):
    assert bool(map_status(status, "")) is True
    assert isinstance(map_status(status, ""), AllowedStatus)


@pytest.mark.parametrize("status", ["Banned", "Probing", "Error", "Unknown"])
async def test_map_status_banned(status):
    result = map_status(status, "CE banned")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "CE banned"


async def test_map_status_unknown_banned():
    result = map_status("WeirdValue", "")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "Unknown status: WeirdValue"
