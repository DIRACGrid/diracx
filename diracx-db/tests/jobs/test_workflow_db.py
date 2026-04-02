"""Tests for the Workflows table and workflow-related methods on JobDB."""

from __future__ import annotations

import hashlib

import pytest

from diracx.core.exceptions import InvalidQueryError
from diracx.db.sql.job.db import JobDB


@pytest.fixture
async def job_db(tmp_path):
    job_db = JobDB("sqlite+aiosqlite:///:memory:")
    async with job_db.engine_context():
        async with job_db.engine.begin() as conn:
            if job_db._db_url.startswith("sqlite"):
                await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
            await conn.run_sync(job_db.metadata.create_all)
        yield job_db


SAMPLE_CWL = """\
cwlVersion: v1.2
class: CommandLineTool
label: test-job
hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User
inputs:
  - id: message
    type: string
outputs: []
"""


def _workflow_id(cwl: str) -> str:
    return hashlib.sha256(cwl.encode()).hexdigest()


async def test_insert_workflow(job_db):
    """Test inserting a workflow and retrieving it."""
    wf_id = _workflow_id(SAMPLE_CWL)

    async with job_db as db:
        inserted = await db.insert_workflow(wf_id, SAMPLE_CWL)
        assert inserted is True

        cwl = await db.get_workflow(wf_id)
        assert cwl == SAMPLE_CWL


async def test_insert_workflow_idempotent(job_db):
    """Inserting the same workflow twice should be a no-op the second time."""
    wf_id = _workflow_id(SAMPLE_CWL)

    async with job_db as db:
        first = await db.insert_workflow(wf_id, SAMPLE_CWL)
        second = await db.insert_workflow(wf_id, SAMPLE_CWL)
        assert first is True
        assert second is False


async def test_get_workflow_not_found(job_db):
    """Getting a non-existent workflow should raise."""
    async with job_db as db:
        with pytest.raises(InvalidQueryError):
            await db.get_workflow("nonexistent" * 4)


async def test_set_workflow_ref(job_db):
    """Test linking a job to a workflow with params."""
    wf_id = _workflow_id(SAMPLE_CWL)

    async with job_db as db:
        # Create workflow
        await db.insert_workflow(wf_id, SAMPLE_CWL)

        # Create a job
        job_id = await db.create_job("compressed_jdl")
        await db.insert_job_attributes(
            {job_id: {"JobID": job_id, "Status": "Received", "VO": "lhcb"}}
        )

        # Link job to workflow
        params = {"message": "hello world"}
        await db.set_workflow_ref(job_id, wf_id, workflow_params=params)


async def test_insert_workflow_persistent(job_db):
    """Test inserting a persistent workflow."""
    wf_id = _workflow_id(SAMPLE_CWL)

    async with job_db as db:
        await db.insert_workflow(wf_id, SAMPLE_CWL, persistent=True)
        cwl = await db.get_workflow(wf_id)
        assert cwl == SAMPLE_CWL
