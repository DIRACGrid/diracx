"""Tests for read adapter response translation."""

from __future__ import annotations

from rss_read_adapter.response_translation import (
    translate_computing_element_status,
    translate_fts_status,
    translate_site_status,
    translate_storage_element_status,
)

from diracx.core.models.rss import (
    AllowedStatus,
    BannedStatus,
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)


def test_translate_storage_element_status():
    """Test translating storage element status."""
    response = {
        "SE1": StorageElementStatus(
            read=AllowedStatus(allowed=True),
            write=AllowedStatus(allowed=True, warnings="Degraded"),
            check=BannedStatus(allowed=False, reason="Banned"),
            remove=AllowedStatus(allowed=True),
        )
    }

    result = translate_storage_element_status(response)
    expected = [
        ("SE1", "StorageElement", "ReadAccess", "Active", None),
        ("SE1", "StorageElement", "WriteAccess", "Degraded", None),
        ("SE1", "StorageElement", "CheckAccess", "Banned", None),
        ("SE1", "StorageElement", "RemoveAccess", "Active", None),
    ]
    assert result == expected


def test_translate_computing_element_status():
    """Test translating computing element status."""
    response = {
        "CE1": ComputeElementStatus(all=AllowedStatus(allowed=True)),
        "CE2": ComputeElementStatus(all=BannedStatus(allowed=False, reason="Error")),
    }

    result = translate_computing_element_status(response)
    expected = [
        ("CE1", "ComputeElement", "all", "Active", None),
        ("CE2", "ComputeElement", "all", "Error", None),
    ]
    assert result == expected


def test_translate_fts_status():
    """Test translating FTS status."""
    response = {
        "FTS1": FTSStatus(all=AllowedStatus(allowed=True)),
        "FTS2": FTSStatus(all=BannedStatus(allowed=False, reason="Probing")),
    }

    result = translate_fts_status(response)
    expected = [
        ("FTS1", "FTS", "all", "Active", None),
        ("FTS2", "FTS", "all", "Probing", None),
    ]
    assert result == expected


def test_translate_site_status():
    """Test translating site status."""
    response = {
        "Site1": SiteStatus(all=AllowedStatus(allowed=True)),
        "Site2": SiteStatus(all=BannedStatus(allowed=False)),
    }

    result = translate_site_status(response)
    expected = [
        ("Site1", "Active"),
        ("Site2", "Banned"),
    ]
    assert result == expected
