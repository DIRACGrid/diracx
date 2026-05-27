from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from diracx.core.models import JobLoggingRecord, JobStatus
from diracx.db.sql import JobLoggingDB


@pytest.fixture
async def job_logging_db():
    job_logging_db = JobLoggingDB("sqlite+aiosqlite:///:memory:")
    async with job_logging_db.engine_context():
        async with job_logging_db.engine.begin() as conn:
            await conn.run_sync(job_logging_db.metadata.create_all)
        yield job_logging_db


async def test_insert_records(job_logging_db: JobLoggingDB):
    date_1 = datetime.now(timezone.utc) - timedelta(minutes=1)
    date_2 = datetime.now(timezone.utc) - timedelta(seconds=1)
    date_3 = datetime.now(timezone.utc)

    async with job_logging_db as job_logging_db:
        records = []
        for i in range(50):
            records.append(
                JobLoggingRecord(
                    job_id=i,
                    status=JobStatus.RECEIVED,
                    minor_status="received_minor_status",
                    application_status="application_status",
                    date=date_1,
                    source="pytest",
                )
            )
            records.append(
                JobLoggingRecord(
                    job_id=i,
                    status=JobStatus.SUBMITTING,
                    minor_status="submitted_minor_status",
                    application_status="application_status",
                    date=date_2,
                    source="pytest",
                )
            )
            records.append(
                JobLoggingRecord(
                    job_id=i,
                    status=JobStatus.RUNNING,
                    minor_status="running_minor_status",
                    application_status="application_status",
                    date=date_3,
                    source="pytest",
                )
            )
        await job_logging_db.insert_records(records)

    async with job_logging_db as job_logging_db:
        res = await job_logging_db.get_records([i for i in range(50)])

        assert len(res) == 50
        # Check the first job - first record
        assert res[0][0].Status == JobStatus.RECEIVED.value
        assert res[0][0].MinorStatus == "received_minor_status"
        assert res[0][0].ApplicationStatus == "application_status"
        assert res[0][0].StatusTime == date_1
        assert res[0][0].Source == "pytest"

        # Check the first job - second record
        assert res[0][1].Status == JobStatus.SUBMITTING.value
        assert res[0][1].MinorStatus == "submitted_minor_status"
        assert res[0][1].ApplicationStatus == "application_status"
        assert res[0][1].StatusTime == date_2
        assert res[0][1].Source == "pytest"

        # Check the first job - third record
        assert res[0][2].Status == JobStatus.RUNNING.value
        assert res[0][2].MinorStatus == "running_minor_status"
        assert res[0][2].ApplicationStatus == "application_status"
        assert res[0][2].StatusTime == date_3
        assert res[0][2].Source == "pytest"

        # Check the last job - third record
        assert res[49][2].Status == JobStatus.RUNNING.value
        assert res[49][2].MinorStatus == "running_minor_status"
        assert res[49][2].ApplicationStatus == "application_status"
        assert res[49][2].StatusTime == date_3
        assert res[49][2].Source == "pytest"

    async with job_logging_db as job_logging_db:
        res = await job_logging_db.get_wms_time_stamps([1])
        assert abs(res[1]["Received"] - date_1) < timedelta(microseconds=1000)
        assert abs(res[1]["Submitting"] - date_2) < timedelta(microseconds=1000)
        assert abs(res[1]["Running"] - date_3) < timedelta(microseconds=1000)
