from __future__ import annotations

import json
import os
import tempfile
from io import StringIO

import pytest
from pytest import raises

from diracx import cli
from diracx.core.preferences import get_diracx_preferences

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
async def jdl_file():
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as temp_file:
        temp_file.write(TEST_JDL)
        temp_file.flush()
        yield temp_file.name


async def test_submit(with_cli_login, jdl_file, capfd):
    """Test submitting a job using a JDL file."""
    with open(jdl_file, "r") as temp_file:
        await cli.jobs.submit([temp_file])

    cap = capfd.readouterr()
    assert cap.err == ""
    assert "Inserted 1 jobs with ids" in cap.out


async def test_search(with_cli_login, jdl_file, capfd):
    """Test searching for jobs."""
    # Submit 20 jobs
    with open(jdl_file, "r") as x:
        what_we_submit = x.read()
    jdls = [StringIO(what_we_submit) for _ in range(20)]

    await cli.jobs.submit(jdls)

    cap = capfd.readouterr()

    # By default the output should be in JSON format as capfd is not a TTY
    await cli.jobs.search()
    cap = capfd.readouterr()
    assert cap.err == ""
    jobs = json.loads(cap.out)

    # There should be 10 jobs by default
    assert len(jobs) == 10
    assert "JobID" in jobs[0]
    assert "JobGroup" in jobs[0]

    # Change per-page to a very large number to get all the jobs at once: the caption should change
    await cli.jobs.search(per_page=9999)
    cap = capfd.readouterr()
    assert cap.err == ""
    jobs = json.loads(cap.out)

    # There should be 20 jobs at least now
    assert len(jobs) >= 20
    assert "JobID" in cap.out
    assert "JobGroup" in cap.out

    # Search for a job that doesn't exist
    await cli.jobs.search(condition=["Status eq nonexistent"])
    cap = capfd.readouterr()
    assert cap.err == ""
    assert "[]" == cap.out.strip()

    # Switch to RICH output
    get_diracx_preferences.cache_clear()
    os.environ["DIRACX_OUTPUT_FORMAT"] = "RICH"

    await cli.jobs.search()
    cap = capfd.readouterr()
    assert cap.err == ""

    with raises(json.JSONDecodeError):
        json.loads(cap.out)

    assert "JobID" in cap.out
    assert "JobGroup" in cap.out
    assert "Showing 0-9 of " in cap.out

    # Change per-page to a very large number to get all the jobs at once: the caption should change
    await cli.jobs.search(per_page=9999)
    cap = capfd.readouterr()
    assert cap.err == ""

    with raises(json.JSONDecodeError):
        json.loads(cap.out)

    assert "JobID" in cap.out
    assert "JobGroup" in cap.out
    assert "Showing all jobs" in cap.out

    # Search for a job that doesn't exist
    await cli.jobs.search(condition=["Status eq nonexistent"])
    cap = capfd.readouterr()
    assert cap.err == ""
    assert "No jobs found" in cap.out
