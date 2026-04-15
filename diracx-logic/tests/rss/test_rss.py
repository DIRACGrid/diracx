from __future__ import annotations

import pytest

from diracx.core.models.rss import (
    AllowedStatus,
    BannedStatus,
)
from diracx.logic.rss.query import map_status


async def test_map_status_allowed():
    result = map_status("Active", "Resource active")
    assert bool(result) is True
    assert isinstance(result, AllowedStatus)
    assert result.warnings is None

    result = map_status("Degraded", "Resource degraded")
    assert bool(result) is True
    assert isinstance(result, AllowedStatus)
    assert result.warnings == "Resource degraded"


@pytest.mark.parametrize("status", ["Banned", "Probing", "Error", "Unknown"])
async def test_map_status_banned(status):
    result = map_status(status, "Resource banned")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "Resource banned"


async def test_map_status_unknown_banned():
    result = map_status("WeirdValue", "")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "Unknown status: WeirdValue"
