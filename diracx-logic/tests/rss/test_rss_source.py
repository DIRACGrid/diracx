from __future__ import annotations

from collections import namedtuple
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from diracx.core.models.rss import (
    ComputeElementStatus,
    FTSStatus,
    ResourceType,
    SiteStatus,
    StorageElementStatus,
)
from diracx.db.sql.rss.db import ResourceStatusDB
from diracx.logic.rss.source import (
    ComputeElementStatusSource,
    FTSStatusSource,
    SiteStatusSource,
    StorageElementStatusSource,
)

_MAX_DATE = datetime.fromisoformat("2023-01-01T00:00:00+00:00")


@pytest.fixture
def mock_resource_status_db():
    """Fixture to mock the ResourceStatusDB."""
    db = MagicMock(spec=ResourceStatusDB)
    db.__aenter__ = AsyncMock(return_value=db)
    db.__aexit__ = AsyncMock(return_value=None)
    db.get_resource_status_date = AsyncMock(return_value=(_MAX_DATE, 4))
    db.get_site_status_date = AsyncMock(return_value=(_MAX_DATE, 2))
    return db


async def test_latest_revision(mock_resource_status_db):
    """Test the latest_revision method of ResourceStatusSource."""
    source = ComputeElementStatusSource(db=mock_resource_status_db)

    # Call the method
    revision, modified = await source.latest_revision()

    # Verify the revision is generated correctly
    assert revision == f"{_MAX_DATE.isoformat()}-4"
    assert modified == _MAX_DATE

    # Verify the database call queries this source's status types
    mock_resource_status_db.get_resource_status_date.assert_awaited_once_with(["all"])


async def test_latest_revision_storage_status_types(mock_resource_status_db):
    """Storage revisions must track the access status types, not "all"."""
    source = StorageElementStatusSource(db=mock_resource_status_db)

    await source.latest_revision()

    mock_resource_status_db.get_resource_status_date.assert_awaited_once_with(
        ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
    )


async def test_latest_revision_empty(mock_resource_status_db):
    """An empty table yields a stable sentinel revision instead of failing."""
    mock_resource_status_db.get_resource_status_date = AsyncMock(return_value=(None, 0))
    source = ComputeElementStatusSource(db=mock_resource_status_db)

    revision, modified = await source.latest_revision()

    assert revision == "empty-0"
    assert modified == datetime(1970, 1, 1, tzinfo=timezone.utc)


async def test_latest_revision_site(mock_resource_status_db):
    """Test the latest_revision method of SiteStatusSource."""
    source = SiteStatusSource(db=mock_resource_status_db)

    revision, modified = await source.latest_revision()

    assert revision == f"{_MAX_DATE.isoformat()}-2"
    assert modified == _MAX_DATE
    mock_resource_status_db.get_site_status_date.assert_awaited_once_with()


async def test_read_raw_site(mock_resource_status_db):
    """Test the read_raw method for Site resource type."""
    # Mock the database data
    mock_db_data = [("testSite", "Active", "", "test_vo")]

    # Patch the get_site_statuses method of the database to return the mock data
    mock_resource_status_db.get_site_statuses = AsyncMock(return_value=mock_db_data)

    # Initialize the ResourceStatusSource with the mocked database
    source = SiteStatusSource(db=mock_resource_status_db)

    # Call the read_raw method, which internally calls get_site_statuses from query.py
    result = await source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    # Verify the result matches the expected output
    expected_result = {"testSite": SiteStatus(all={"allowed": True, "warnings": None})}
    for key, value in expected_result.items():
        assert key in result.data["test_vo"]
        assert value.model_dump() == result.data["test_vo"][key].model_dump()
    # Verify that the database method was called correctly
    mock_resource_status_db.get_site_statuses.assert_awaited_once()


async def test_read_raw_compute(mock_resource_status_db):
    """Test the read_raw method for ComputeElement resource type."""
    ResourceStatus = namedtuple("ResourceStatus", ["Name", "Status", "Reason", "VO"])

    mock_db_data = {
        "test_vo": {
            "TestCE": {
                "all": ResourceStatus(
                    Name="TestCE", Status="Active", Reason="", VO="test_vo"
                )
            }
        }
    }
    mock_resource_status_db.get_resource_statuses = AsyncMock(return_value=mock_db_data)

    source = ComputeElementStatusSource(db=mock_resource_status_db)

    # Call the method
    result = await source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    # Verify the result
    expected_result = {
        "TestCE": ComputeElementStatus(all={"allowed": True, "warnings": None})
    }
    for key, value in expected_result.items():
        assert key in result.data["test_vo"]
        assert value.model_dump() == result.data["test_vo"][key].model_dump()
    mock_resource_status_db.get_resource_statuses.assert_awaited_once_with(
        ["all"], element_type=ResourceType.Compute
    )


async def test_read_raw_storage(mock_resource_status_db):
    """Test the read_raw method for StorageElement resource type."""
    ResourceStatus = namedtuple("ResourceStatus", ["Name", "Status", "Reason", "VO"])

    mock_db_data = {
        "test_vo": {
            "TestSE": {
                "ReadAccess": ResourceStatus(
                    Name="TestSE", Status="Active", Reason=None, VO="test_vo"
                ),
                "WriteAccess": ResourceStatus(
                    Name="TestSE", Status="Active", Reason=None, VO="test_vo"
                ),
                "CheckAccess": ResourceStatus(
                    Name="TestSE", Status="Active", Reason=None, VO="test_vo"
                ),
                "RemoveAccess": ResourceStatus(
                    Name="TestSE", Status="Active", Reason=None, VO="test_vo"
                ),
            }
        }
    }
    mock_resource_status_db.get_resource_statuses.return_value = mock_db_data
    source = StorageElementStatusSource(db=mock_resource_status_db)

    # Call the method
    result = await source.read_raw("test_revision", datetime.now(tz=timezone.utc))

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
        assert key in result.data["test_vo"]
        assert value.model_dump() == result.data["test_vo"][key].model_dump()
    mock_resource_status_db.get_resource_statuses.assert_awaited_once_with(
        ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"],
        element_type=ResourceType.Storage,
    )


async def test_read_raw_storage_partial_rows_skipped(mock_resource_status_db, caplog):
    """A storage element missing access rows is skipped rather than failing."""
    ResourceStatus = namedtuple("ResourceStatus", ["Name", "Status", "Reason", "VO"])

    mock_db_data = {
        "test_vo": {
            "PartialSE": {
                "ReadAccess": ResourceStatus(
                    Name="PartialSE", Status="Active", Reason=None, VO="test_vo"
                ),
            }
        }
    }
    mock_resource_status_db.get_resource_statuses.return_value = mock_db_data
    source = StorageElementStatusSource(db=mock_resource_status_db)

    result = await source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    assert result.data == {"test_vo": {}}
    assert "PartialSE" in caplog.text


async def test_read_raw_fts(mock_resource_status_db):
    """Test the read_raw method for FTS resource type."""
    ResourceStatus = namedtuple("ResourceStatus", ["Name", "Status", "Reason", "VO"])

    mock_db_data = {
        "test_vo": {
            "FTS": {
                "all": ResourceStatus(
                    Name="FTS", Status="Active", Reason=None, VO="test_vo"
                ),
            }
        }
    }
    mock_resource_status_db.get_resource_statuses.return_value = mock_db_data

    source = FTSStatusSource(db=mock_resource_status_db)

    # Call the method
    result = await source.read_raw("test_revision", datetime.now(tz=timezone.utc))

    # Verify the result
    expected_result = {"FTS": FTSStatus(all={"allowed": True, "warnings": None})}
    for key, value in expected_result.items():
        assert key in result.data["test_vo"]
        assert value.model_dump() == result.data["test_vo"][key].model_dump()
    mock_resource_status_db.get_resource_statuses.assert_awaited_once_with(
        ["all"], element_type=ResourceType.FTS
    )
