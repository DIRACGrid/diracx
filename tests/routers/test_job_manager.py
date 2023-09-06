from http import HTTPStatus

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

TEST_LARGE_PARAMETRIC_JDL = """
    Executable = "echo";
    Arguments = "%s";
    JobName = "Test_%n";
    Parameters = 100;
    ParameterStart = 1;
"""


def test_insert_and_list_parametric_jobs(normal_user_client):
    job_definitions = [TEST_PARAMETRIC_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.post("/jobs/search")
    assert r.status_code == 200, r.json()

    listed_jobs = r.json()

    assert len(listed_jobs) == 3  # Parameters.JOB_ID is 3

    assert submitted_job_ids == sorted([job_dict["JobID"] for job_dict in listed_jobs])


@pytest.mark.parametrize(
    "job_definitions",
    [
        [TEST_JDL],
        [TEST_JDL for _ in range(2)],
        [TEST_JDL for _ in range(10)],
    ],
)
def test_insert_and_list_bulk_jobs(job_definitions, normal_user_client):
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.post("/jobs/search")
    assert r.status_code == 200, r.json()

    listed_jobs = r.json()

    assert len(listed_jobs) == len(job_definitions)

    assert submitted_job_ids == sorted([job_dict["JobID"] for job_dict in listed_jobs])


def test_insert_and_search(normal_user_client):
    # job_definitions = [TEST_JDL%(normal_user_client.dirac_token_payload)]
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    # Test /jobs/search
    r = normal_user_client.post("/jobs/search")
    assert r.status_code == 200, r.json()
    assert [x["JobID"] for x in r.json()] == submitted_job_ids
    assert {x["VerifiedFlag"] for x in r.json()} == {True}

    r = normal_user_client.post(
        "/jobs/search",
        json={"search": [{"parameter": "Status", "operator": "eq", "value": "NEW"}]},
    )
    assert r.status_code == 200, r.json()
    assert r.json() == []

    r = normal_user_client.post(
        "/jobs/search",
        json={
            "search": [{"parameter": "Status", "operator": "eq", "value": "RECEIVED"}]
        },
    )
    assert r.status_code == 200, r.json()
    assert [x["JobID"] for x in r.json()] == submitted_job_ids

    r = normal_user_client.post(
        "/jobs/search", json={"parameters": ["JobID", "Status"]}
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [
        {"JobID": jid, "Status": "RECEIVED"} for jid in submitted_job_ids
    ]

    # Test /jobs/summary
    r = normal_user_client.post(
        "/jobs/summary", json={"grouping": ["Status", "OwnerGroup"]}
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [{"Status": "RECEIVED", "OwnerGroup": "test_group", "count": 1}]

    r = normal_user_client.post(
        "/jobs/summary",
        json={
            "grouping": ["Status"],
            "search": [{"parameter": "Status", "operator": "eq", "value": "RECEIVED"}],
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [{"Status": "RECEIVED", "count": 1}]

    r = normal_user_client.post(
        "/jobs/summary",
        json={
            "grouping": ["Status"],
            "search": [{"parameter": "Status", "operator": "eq", "value": "NEW"}],
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json() == []


def test_user_cannot_submit_parametric_jdl_greater_than_max_parametric_jobs(
    normal_user_client,
):
    """Test that a user cannot submit a parametric JDL greater than the max parametric jobs"""
    job_definitions = [TEST_LARGE_PARAMETRIC_JDL]
    res = normal_user_client.post("/jobs/", json=job_definitions)
    assert res.status_code == HTTPStatus.UNAUTHORIZED, res.json()


def test_user_cannot_submit_list_of_jdl_greater_than_max_number_of_jobs(
    normal_user_client,
):
    """Test that a user cannot submit a list of JDL greater than the max number of jobs"""
    job_definitions = [TEST_JDL for _ in range(100)]
    res = normal_user_client.post("/jobs/", json=job_definitions)
    assert res.status_code == HTTPStatus.UNAUTHORIZED, res.json()


@pytest.mark.parametrize(
    "job_definitions",
    [[TEST_PARAMETRIC_JDL, TEST_JDL], [TEST_PARAMETRIC_JDL, TEST_PARAMETRIC_JDL]],
)
def test_user_cannot_submit_multiple_jdl_if_at_least_one_of_them_is_parametric(
    normal_user_client, job_definitions
):
    res = normal_user_client.post("/jobs/", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


def test_user_without_the_normal_user_property_cannot_submit_job(admin_user_client):
    res = admin_user_client.post("/jobs/", json=[TEST_JDL])
    assert res.status_code == HTTPStatus.FORBIDDEN, res.json()


def test_insert_and_reschedule(normal_user_client: TestClient):
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    # Test /jobs/reschedule
    r = normal_user_client.post(
        "/jobs/reschedule",
        params={"job_ids": submitted_job_ids},
    )
    assert r.status_code == 200, r.json()
