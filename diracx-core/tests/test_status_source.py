from __future__ import annotations

from collections import namedtuple
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from diracx.core.config.sources import ResourceStatusSource
from diracx.core.exceptions import ResourceNotFoundError
from diracx.core.models.rss import (
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)
from diracx.db.sql.rss.db import ResourceStatusDB


@pytest.fixture
def mock_resource_status_db():
    """Fixture to mock the ResourceStatusDB."""
    db = MagicMock(spec=ResourceStatusDB)
    DateRow = namedtuple("DateRow", ["DateEffective", "DateChecked"])
    db.get_status_date = AsyncMock(
        return_value=DateRow(
            DateEffective=datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
            DateChecked=datetime.now(timezone.utc),
        )
    )
    return db


def test_latest_revision(mock_resource_status_db):
    """Test the latest_revision method of ResourceStatusSource."""
    source = ResourceStatusSource(
        resource_type="Site",
        vo="test_vo",
        resource_status_db=mock_resource_status_db,
    )

    # Call the method
    revision, modified = source.latest_revision()

    # Verify the revision is generated correctly
    assert revision.startswith("rev_")
    assert isinstance(modified, datetime)

    # Verify the database call
    mock_resource_status_db.get_status_date.assert_called_once_with(vo="test_vo")


def test_read_raw_site(mock_resource_status_db):
    """Test the read_raw method for Site resource type."""
    # Mock the database data
    mock_db_data = [("testSite", "Active", "")]

    # Patch the get_site_statuses method of the database to return the mock data
    mock_resource_status_db.get_site_statuses = AsyncMock(return_value=mock_db_data)

    # Initialize the ResourceStatusSource with the mocked database
    source = ResourceStatusSource(
        resource_type="Site",
        vo="test_vo",
        resource_status_db=mock_resource_status_db,
    )

    # Call the read_raw method, which internally calls get_site_statuses from query.py
    result = source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    # Verify the result matches the expected output
    expected_result = {"testSite": SiteStatus(all={"allowed": True, "warnings": None})}
    for key, value in expected_result.items():
        assert key in result
        assert value.model_dump() == result[key].model_dump()
    # Verify that the database method was called correctly
    mock_resource_status_db.get_site_statuses.assert_awaited_once_with("test_vo")


def test_read_raw_compute(mock_resource_status_db):
    """Test the read_raw method for ComputeElement resource type."""
    ResourceStatus = namedtuple("ResourceStatus", ["Name", "Status", "Reason"])

    mock_db_data = {
        "TestCE": {"all": ResourceStatus(Name="TestCE", Status="Active", Reason="")}
    }
    mock_resource_status_db.get_resource_statuses = AsyncMock(return_value=mock_db_data)

    source = ResourceStatusSource(
        resource_type="ComputeElement",
        vo="test_vo",
        resource_status_db=mock_resource_status_db,
    )

    # Call the method
    result = source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    # Verify the result
    expected_result = {
        "TestCE": ComputeElementStatus(all={"allowed": True, "warnings": None})
    }
    for key, value in expected_result.items():
        assert key in result
        assert value.model_dump() == result[key].model_dump()
    mock_resource_status_db.get_resource_statuses.assert_awaited_once_with(
        ["all"], "test_vo"
    )


def test_read_raw_storage(mock_resource_status_db):
    """Test the read_raw method for StorageElement resource type."""
    ResourceStatus = namedtuple("ResourceStatus", ["Name", "Status", "Reason"])

    mock_db_data = {
        "TestSE": {
            "ReadAccess": ResourceStatus(Name="TestSE", Status="Active", Reason=None),
            "WriteAccess": ResourceStatus(Name="TestSE", Status="Active", Reason=None),
            "CheckAccess": ResourceStatus(Name="TestSE", Status="Active", Reason=None),
            "RemoveAccess": ResourceStatus(Name="TestSE", Status="Active", Reason=None),
        }
    }
    mock_resource_status_db.get_resource_statuses.return_value = mock_db_data
    source = ResourceStatusSource(
        resource_type="StorageElement",
        vo="test_vo",
        resource_status_db=mock_resource_status_db,
    )

    # Call the method
    result = source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    # Verify the result
    expected_result = {
        "TestSE": StorageElementStatus(
            read={"allowed": True, "warnings": None},
            write={"allowed": True, "warnings": None},
            check={"allowed": True, "warnings": None},
            remove={"allowed": True, "warnings": None},
        )
    }
    for key, value in expected_result.items():
        assert key in result
        assert value.model_dump() == result[key].model_dump()
    mock_resource_status_db.get_resource_statuses.assert_awaited_once_with(
        ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"], "test_vo"
    )


def test_read_raw_fts(mock_resource_status_db):
    """Test the read_raw method for FTS resource type."""
    ResourceStatus = namedtuple("ResourceStatus", ["Name", "Status", "Reason"])

    mock_db_data = {
        "FTS": {
            "all": ResourceStatus(Name="FTS", Status="Active", Reason=None),
        }
    }
    mock_resource_status_db.get_resource_statuses.return_value = mock_db_data

    source = ResourceStatusSource(
        resource_type="FTS",
        vo="test_vo",
        resource_status_db=mock_resource_status_db,
    )

    # Call the method
    result = source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    # Verify the result
    expected_result = {"FTS": FTSStatus(all={"allowed": True, "warnings": None})}
    for key, value in expected_result.items():
        assert key in result
        assert value.model_dump() == result[key].model_dump()
    mock_resource_status_db.get_resource_statuses.assert_awaited_once_with(
        ["all"], "test_vo"
    )


def test_read_raw_invalid_resource_type(mock_resource_status_db):
    """Test the read_raw method for an invalid resource type."""
    source = ResourceStatusSource(
        resource_type="InvalidType",
        vo="test_vo",
        resource_status_db=mock_resource_status_db,
    )

    # Call the method and verify it raises ResourceNotFoundError
    with pytest.raises(ResourceNotFoundError):
        source.read_raw("test_revision", datetime.now(tz=timezone.utc))
