from datetime import datetime, timezone
from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient

from diracx.core.models import JobStatus

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
            "search": [
                {
                    "parameter": "Status",
                    "operator": "eq",
                    "value": JobStatus.RECEIVED.value,
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert [x["JobID"] for x in r.json()] == submitted_job_ids

    r = normal_user_client.post(
        "/jobs/search", json={"parameters": ["JobID", "Status"]}
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [
        {"JobID": jid, "Status": JobStatus.RECEIVED.value} for jid in submitted_job_ids
    ]

    # Test /jobs/summary
    r = normal_user_client.post(
        "/jobs/summary", json={"grouping": ["Status", "OwnerGroup"]}
    )
    assert r.status_code == 200, r.json()

    assert r.json() == [
        {"Status": JobStatus.RECEIVED.value, "OwnerGroup": "test_group", "count": 1}
    ]

    r = normal_user_client.post(
        "/jobs/summary",
        json={
            "grouping": ["Status"],
            "search": [
                {
                    "parameter": "Status",
                    "operator": "eq",
                    "value": JobStatus.RECEIVED.value,
                }
            ],
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [{"Status": JobStatus.RECEIVED.value, "count": 1}]

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
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


def test_user_cannot_submit_list_of_jdl_greater_than_max_number_of_jobs(
    normal_user_client,
):
    """Test that a user cannot submit a list of JDL greater than the max number of jobs"""
    job_definitions = [TEST_JDL for _ in range(100)]
    res = normal_user_client.post("/jobs/", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


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


def test_get_job_status(normal_user_client: TestClient):
    """Test that the job status is returned correctly."""
    # Arrange
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1  # Parameters.JOB_ID is 3
    job_id = r.json()[0]["JobID"]

    # Act
    r = normal_user_client.get(f"/jobs/{job_id}/status")

    # Assert
    assert r.status_code == 200, r.json()
    # TODO: should we return camel case here (and everywhere else) ?
    assert r.json()[str(job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


def test_get_status_of_nonexistent_job(normal_user_client: TestClient):
    """Test that the job status is returned correctly."""
    # Act
    r = normal_user_client.get("/jobs/1/status")

    # Assert
    assert r.status_code == 404, r.json()
    assert r.json() == {"detail": "Job 1 not found"}


def test_get_job_status_in_bulk(normal_user_client: TestClient):
    """Test that we can get the status of multiple jobs in one request"""
    # Arrange
    job_definitions = [TEST_PARAMETRIC_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3
    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])
    assert isinstance(submitted_job_ids, list)
    assert (isinstance(submitted_job_id, int) for submitted_job_id in submitted_job_ids)

    # Act
    r = normal_user_client.get("/jobs/status", params={"job_ids": submitted_job_ids})

    # Assert
    print(r.json())
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3
    for job_id in submitted_job_ids:
        assert str(job_id) in r.json()
        assert r.json()[str(job_id)]["Status"] == JobStatus.SUBMITTING.value
        assert r.json()[str(job_id)]["MinorStatus"] == "Bulk transaction confirmation"
        assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


async def test_get_job_status_history(normal_user_client: TestClient):
    # Arrange
    job_definitions = [TEST_JDL]
    before = datetime.now(timezone.utc)
    r = normal_user_client.post("/jobs/", json=job_definitions)
    after = datetime.now(timezone.utc)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    job_id = r.json()[0]["JobID"]
    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"

    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    beforebis = datetime.now(timezone.utc)
    r = normal_user_client.put(
        f"/jobs/{job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": NEW_STATUS,
                "MinorStatus": NEW_MINOR_STATUS,
            }
        },
    )
    afterbis = datetime.now(timezone.utc)
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    # Act
    r = normal_user_client.get(
        f"/jobs/{job_id}/status/history",
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    assert len(r.json()[str(job_id)]) == 2
    assert r.json()[str(job_id)][0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)][0]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)][0]["ApplicationStatus"] == "Unknown"
    assert (
        before < datetime.fromisoformat(r.json()[str(job_id)][0]["StatusTime"]) < after
    )
    assert r.json()[str(job_id)][0]["StatusSource"] == "JobManager"

    assert r.json()[str(job_id)][1]["Status"] == JobStatus.CHECKING.value
    assert r.json()[str(job_id)][1]["MinorStatus"] == "JobPath"
    assert r.json()[str(job_id)][1]["ApplicationStatus"] == "Unknown"
    assert (
        beforebis
        < datetime.fromisoformat(r.json()[str(job_id)][1]["StatusTime"])
        < afterbis
    )
    assert r.json()[str(job_id)][1]["StatusSource"] == "Unknown"


def test_get_job_status_history_in_bulk(normal_user_client: TestClient):
    # Arrange
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    job_id = r.json()[0]["JobID"]
    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    r = normal_user_client.get("/jobs/status/history", params={"job_ids": [job_id]})

    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    assert r.json()[str(job_id)][0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)][0]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)][0]["ApplicationStatus"] == "Unknown"
    assert datetime.fromisoformat(r.json()[str(job_id)][0]["StatusTime"])
    assert r.json()[str(job_id)][0]["StatusSource"] == "JobManager"


def test_set_job_status(normal_user_client: TestClient):
    # Arrange
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    job_id = r.json()[0]["JobID"]
    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.put(
        f"/jobs/{job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": NEW_STATUS,
                "MinorStatus": NEW_MINOR_STATUS,
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_invalid_job(normal_user_client: TestClient):
    # Act
    r = normal_user_client.put(
        "/jobs/1/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": JobStatus.CHECKING.value,
                "MinorStatus": "JobPath",
            }
        },
    )

    # Assert
    assert r.status_code == 404, r.json()
    assert r.json() == {"detail": "Job 1 not found"}


def test_set_job_status_offset_naive_datetime_return_bad_request(
    normal_user_client: TestClient,
):
    # Arrange
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    job_id = r.json()[0]["JobID"]

    # Act
    date = datetime.utcnow().isoformat(sep=" ")
    r = normal_user_client.put(
        f"/jobs/{job_id}/status",
        json={
            date: {
                "Status": JobStatus.CHECKING.value,
                "MinorStatus": "JobPath",
            }
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.BAD_REQUEST, r.json()
    assert r.json() == {"detail": f"Timestamp {date} is not timezone aware"}


def test_set_job_status_cannot_make_impossible_transitions(
    normal_user_client: TestClient,
):
    # Arrange
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    job_id = r.json()[0]["JobID"]
    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.RUNNING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.put(
        f"/jobs/{job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": NEW_STATUS,
                "MinorStatus": NEW_MINOR_STATUS,
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] != NEW_STATUS
    assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] != NEW_STATUS
    assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_force(normal_user_client: TestClient):
    # Arrange
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    job_id = r.json()[0]["JobID"]
    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.RUNNING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.put(
        f"/jobs/{job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": NEW_STATUS,
                "MinorStatus": NEW_MINOR_STATUS,
            }
        },
        params={"force": True},
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.get(f"/jobs/{job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_bulk(normal_user_client: TestClient):
    # Arrange
    job_definitions = [TEST_PARAMETRIC_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3
    job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    for job_id in job_ids:
        r = normal_user_client.get(f"/jobs/{job_id}/status")
        assert r.status_code == 200, r.json()
        assert r.json()[str(job_id)]["Status"] == JobStatus.SUBMITTING.value
        assert r.json()[str(job_id)]["MinorStatus"] == "Bulk transaction confirmation"

    # Act
    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.put(
        "/jobs/status",
        json={
            job_id: {
                datetime.now(timezone.utc).isoformat(): {
                    "Status": NEW_STATUS,
                    "MinorStatus": NEW_MINOR_STATUS,
                }
            }
            for job_id in job_ids
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    for job_id in job_ids:
        assert r.json()[str(job_id)]["Status"] == NEW_STATUS
        assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS

        r_get = normal_user_client.get(f"/jobs/{job_id}/status")
        assert r_get.status_code == 200, r_get.json()
        assert r_get.json()[str(job_id)]["Status"] == NEW_STATUS
        assert r_get.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS
        assert r_get.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_with_invalid_job_id(normal_user_client: TestClient):
    # Act
    r = normal_user_client.put(
        "/jobs/999999999/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": JobStatus.CHECKING.value,
                "MinorStatus": "JobPath",
            },
        },
    )

    # Assert
    assert r.status_code == 404, r.json()
    assert r.json() == {"detail": "Job 999999999 not found"}


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
