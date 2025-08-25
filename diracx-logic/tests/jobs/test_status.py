from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import sqlalchemy

from diracx.core.models import JobMetaData
from diracx.db.os.job_parameters import JobParametersDB as RealJobParametersDB
from diracx.db.sql.job.db import JobDB
from diracx.logic.jobs.status import set_job_parameters_or_attributes
from diracx.testing.mock_osdb import MockOSDBMixin
from diracx.testing.time import mock_sqlite_time


# Reuse the generic MockOSDBMixin to build a mock JobParameters DB implementation
class _MockJobParametersDB(MockOSDBMixin, RealJobParametersDB):
    def __init__(self):  # type: ignore[override]
        super().__init__({"sqlalchemy_dsn": "sqlite+aiosqlite:///:memory:"})

    def upsert(self, vo, doc_id, document):
        """Override to add JobID to the document."""
        # Add JobID to the document, which is required by the base class
        document["JobID"] = doc_id
        return super().upsert(vo, doc_id, document)


# --------------------------------------------------------------------------------------
# Test setup fixtures
# --------------------------------------------------------------------------------------


@pytest.fixture
async def job_db() -> AsyncGenerator[JobDB, None]:
    """Create a fake sandbox metadata database."""
    db = JobDB(db_url="sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        sqlalchemy.event.listen(db.engine.sync_engine, "connect", mock_sqlite_time)

        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)

        yield db


@pytest.fixture
async def job_parameters_db() -> AsyncGenerator[_MockJobParametersDB, None]:
    db = _MockJobParametersDB()
    # Need engine_context entered before creating tables
    async with db.client_context():
        await db.create_index_template()
        yield db


TEST_JDL = """
    Arguments = "jobDescription.xml -o LogLevel=INFO";
    Executable = "dirac-jobexec";
    JobGroup = jobGroup;
    JobName = jobName;
    JobType = User;
    LogLevel = INFO;
    OutputSandbox =
        {
            Script1_CodeOutput.log,
            std.err,
            std.out
        };
    Priority = 1;
    Site = ANY;
    StdError = std.err;
    StdOutput = std.out;
"""


@pytest.fixture
async def valid_job_id(job_db: JobDB) -> int:
    """Create a minimal job record and return its JobID."""
    async with job_db:
        job_id = await job_db.create_job("")  # original JDL unused in these tests
        # Insert initial attributes (mimic job submission)
        await job_db.insert_job_attributes(
            {
                job_id: {
                    "Status": "Received",
                    "MinorStatus": "Job accepted",
                    "ApplicationStatus": "Unknown",
                    "VO": "lhcb",
                    "Owner": "tester",
                    "OwnerGroup": "lhcb_user",
                    "VerifiedFlag": True,
                    "JobType": "User",
                }
            }
        )
    return job_id


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_patch_metadata_updates_attributes_and_parameters(
    job_db: JobDB, job_parameters_db: _MockJobParametersDB, valid_job_id: int
):
    """Patch metadata mixing:
    - Attribute only (UserPriority)
    - Attribute + parameter (JobType)
    - Parameter only (CPUNormalizationFactor)
    - Attribute (HeartBeatTime)
    - Non identified Metadata (does_not_exist)
    and verify correct persistence in the two backends.
    """
    hbt = datetime.now(timezone.utc)

    metadata = {
        "UserPriority": "2",  # attr
        "JobType": "VerySpecialIndeed",  # attr + param
        "CPUNormalizationFactor": "10",  # param only
        "HeartBeatTime": hbt.isoformat(),  # attr
        "does_not_exist": "unknown",  # Does not exist should be treated as a job param
    }

    updates = {valid_job_id: JobMetaData.model_validate(metadata)}

    # Act
    async with job_db:  # ensure open connection for updates
        await set_job_parameters_or_attributes(updates, job_db, job_parameters_db)

    # Assert job attributes (SQL)
    async with job_db:
        _, rows = await job_db.search(
            parameters=None,
            search=[{"parameter": "JobID", "operator": "eq", "value": valid_job_id}],
            sorts=[],
        )
    assert len(rows) == 1
    row = rows[0]
    assert int(row["JobID"]) == valid_job_id
    assert row["UserPriority"] == 2
    assert row["JobType"] == "VerySpecialIndeed"
    # HeartBeatTime stored as ISO string (without tz) in DB helper; just ensure present
    assert row["HeartBeatTime"] is not None
    assert "CPUNormalizationFactor" not in row
    assert "does_not_exist" not in row

    # Assert job parameters (mocked OS / sqlite)
    params_rows = await job_parameters_db.search(
        parameters=None,
        search=[{"parameter": "JobID", "operator": "eq", "value": valid_job_id}],
        sorts=[],
    )
    prow = params_rows[0]
    assert prow["JobType"] == "VerySpecialIndeed"
    assert prow["CPUNormalizationFactor"] == 10
    assert prow["does_not_exist"] == "unknown"
    assert "UserPriority" not in prow
    assert "HeartBeatTime" not in prow
