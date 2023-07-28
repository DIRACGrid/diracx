from __future__ import annotations

import pytest

from diracx.db.job_logging.db import JobLoggingDB


@pytest.fixture
async def job_logging_db(tmp_path):
    job_logging_db = JobLoggingDB("sqlite+aiosqlite:///:memory:")
    async with job_logging_db.engine_context():
        yield job_logging_db
