from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

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

TEST_PARAMETRIC_JDL = """
Arguments = "jobDescription.xml -o LogLevel=DEBUG  -p JOB_ID=%(JOB_ID)s  -p InputData=%(InputData)s";
    Executable = "dirac-jobexec";
    InputData = %(InputData)s;
    InputSandbox = jobDescription.xml;
    JOB_ID = %(JOB_ID)s;
    JobName = Name;
    JobType = User;
    LogLevel = DEBUG;
    OutputSandbox =
        {
            Script1_CodeOutput.log,
            std.err,
            std.out
        };
    Parameters = 3;
    Parameters.InputData =
        {
            {/lhcb/data/data1,
            /lhcb/data/data2},
            {/lhcb/data/data3,
            /lhcb/data/data4},
            {/lhcb/data/data5,
            /lhcb/data/data6}
        };
    Parameters.JOB_ID =
        {
            1,
            2,
            3
        };
    Priority = 1;
    StdError = std.err;
    StdOutput = std.out;
"""


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


@pytest.fixture
def admin_user_client(client_factory):
    with client_factory.admin_user() as client:
        yield client


@pytest.fixture
def valid_job_id(normal_user_client: TestClient):
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    return r.json()[0]["JobID"]


@pytest.fixture
def valid_job_ids(normal_user_client: TestClient):
    job_definitions = [TEST_PARAMETRIC_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3
    return sorted([job_dict["JobID"] for job_dict in r.json()])


@pytest.fixture
def invalid_job_id():
    return 999999996


@pytest.fixture
def invalid_job_ids():
    return [999999997, 999999998, 999999999]
