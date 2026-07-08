"""Tests for read adapter VO handling."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from rss_read_adapter.statuses import (
    get_computing_element_status,
    get_fts_status,
    get_site_status,
    get_storage_element_status,
)


@pytest.mark.asyncio
async def test_get_storage_element_status():
    """Test getting storage element status with proper response format."""
    from diracx.core.models.rss import AllowedStatus, BannedStatus, StorageElementStatus

    # Create mock client with proper async response
    mock_client = MagicMock()
    mock_client.rss.get_storage_status = AsyncMock(
        return_value={
            "SE1": StorageElementStatus(
                read=AllowedStatus(allowed=True),
                write=AllowedStatus(allowed=True, warnings="Degraded"),
                check=BannedStatus(allowed=False, reason="Banned"),
                remove=AllowedStatus(allowed=True),
            )
        }
    )

    result = await get_storage_element_status(mock_client)

    # Verify client method was called exactly once
    mock_client.rss.get_storage_status.assert_awaited_once()

    # Verify result matches expected legacy format
    expected = [
        ("SE1", "StorageElement", "ReadAccess", "Active", None),
        ("SE1", "StorageElement", "WriteAccess", "Degraded", None),
        ("SE1", "StorageElement", "CheckAccess", "Banned", None),
        ("SE1", "StorageElement", "RemoveAccess", "Active", None),
    ]
    assert result == expected


@pytest.mark.asyncio
async def test_get_computing_element_status():
    """Test getting computing element status with proper response format."""
    from diracx.core.models.rss import AllowedStatus, BannedStatus, ComputeElementStatus

    # Create mock client with proper async response
    mock_client = MagicMock()
    mock_client.rss.get_compute_status = AsyncMock(
        return_value={
            "CE1": ComputeElementStatus(all=AllowedStatus(allowed=True)),
            "CE2": ComputeElementStatus(
                all=BannedStatus(allowed=False, reason="Error")
            ),
        }
    )

    result = await get_computing_element_status(mock_client)

    # Verify client method was called exactly once
    mock_client.rss.get_compute_status.assert_awaited_once()

    # Verify result matches expected legacy format
    expected = [
        ("CE1", "ComputeElement", "all", "Active", None),
        ("CE2", "ComputeElement", "all", "Error", None),
    ]
    assert result == expected


@pytest.mark.asyncio
async def test_get_fts_status():
    """Test getting FTS status with proper response format."""
    from diracx.core.models.rss import AllowedStatus, BannedStatus, FTSStatus

    # Create mock client with proper async response
    mock_client = MagicMock()
    mock_client.rss.get_fts_status = AsyncMock(
        return_value={
            "FTS1": FTSStatus(all=AllowedStatus(allowed=True)),
            "FTS2": FTSStatus(all=BannedStatus(allowed=False, reason="Probing")),
        }
    )

    result = await get_fts_status(mock_client)

    # Verify client method was called exactly once
    mock_client.rss.get_fts_status.assert_awaited_once()

    # Verify result matches expected legacy format
    expected = [
        ("FTS1", "FTS", "all", "Active", None),
        ("FTS2", "FTS", "all", "Probing", None),
    ]
    assert result == expected


@pytest.mark.asyncio
async def test_get_site_status():
    """Test getting site status with proper response format."""
    from diracx.core.models.rss import AllowedStatus, BannedStatus, SiteStatus

    # Create mock client with proper async response
    mock_client = MagicMock()
    mock_client.rss.get_site_status = AsyncMock(
        return_value={
            "Site1": SiteStatus(all=AllowedStatus(allowed=True)),
            "Site2": SiteStatus(all=BannedStatus(allowed=False)),
        }
    )

    result = await get_site_status(mock_client)

    # Verify client method was called exactly once
    mock_client.rss.get_site_status.assert_awaited_once()

    # Verify result matches expected legacy format
    expected = [
        ("Site1", "Active"),
        ("Site2", "Banned"),
    ]
    assert result == expected
