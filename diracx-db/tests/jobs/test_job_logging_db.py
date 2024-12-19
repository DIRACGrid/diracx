from datetime import datetime, timezone

import pytest

from diracx.core.models import JobStatus
from diracx.db.sql import JobLoggingDB


@pytest.fixture
async def job_logging_db():
    job_logging_db = JobLoggingDB("sqlite+aiosqlite:///:memory:")
    async with job_logging_db.engine_context():
        async with job_logging_db.engine.begin() as conn:
            await conn.run_sync(job_logging_db.metadata.create_all)
        yield job_logging_db


async def test_insert_record(job_logging_db: JobLoggingDB):
    async with job_logging_db as job_logging_db:
        # Arrange
        date = datetime.now(timezone.utc)

        # Act
        await job_logging_db.insert_record(
            1,
            status=JobStatus.RECEIVED,
            minor_status="minor_status",
            application_status="application_status",
            date=date,
            source="pytest",
        )

        # Assert
        res = await job_logging_db.get_records(1)

        assert len(res) == 1
        assert res[0].Status == JobStatus.RECEIVED.value
        assert res[0].MinorStatus == "minor_status"
        assert res[0].ApplicationStatus == "application_status"
        assert res[0].StatusTime == date
        assert res[0].Source == "pytest"
