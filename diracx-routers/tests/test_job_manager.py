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
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.post("/api/jobs/search")
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
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.post("/api/jobs/search")
    assert r.status_code == 200, r.json()

    listed_jobs = r.json()

    assert len(listed_jobs) == len(job_definitions)

    assert submitted_job_ids == sorted([job_dict["JobID"] for job_dict in listed_jobs])


def test_insert_and_search(normal_user_client):
    # job_definitions = [TEST_JDL%(normal_user_client.dirac_token_payload)]
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    # Test /jobs/search
    r = normal_user_client.post("/api/jobs/search")
    assert r.status_code == 200, r.json()
    assert [x["JobID"] for x in r.json()] == submitted_job_ids
    assert {x["VerifiedFlag"] for x in r.json()} == {True}

    r = normal_user_client.post(
        "/api/jobs/search",
        json={"search": [{"parameter": "Status", "operator": "eq", "value": "NEW"}]},
    )
    assert r.status_code == 200, r.json()
    assert r.json() == []

    r = normal_user_client.post(
        "/api/jobs/search",
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
        "/api/jobs/search", json={"parameters": ["JobID", "Status"]}
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [
        {"JobID": jid, "Status": JobStatus.RECEIVED.value} for jid in submitted_job_ids
    ]

    # Test /jobs/summary
    r = normal_user_client.post(
        "/api/jobs/summary", json={"grouping": ["Status", "OwnerGroup"]}
    )
    assert r.status_code == 200, r.json()

    assert r.json() == [
        {"Status": JobStatus.RECEIVED.value, "OwnerGroup": "test_group", "count": 1}
    ]

    r = normal_user_client.post(
        "/api/jobs/summary",
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
        "/api/jobs/summary",
        json={
            "grouping": ["Status"],
            "search": [{"parameter": "Status", "operator": "eq", "value": "NEW"}],
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json() == []


def test_search_distinct(normal_user_client):
    job_definitions = [TEST_JDL, TEST_JDL, TEST_JDL]
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    # Check that distinct collapses identical records when true
    r = normal_user_client.post(
        "/api/jobs/search", json={"parameters": ["Status"], "distinct": False}
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) > 1
    r = normal_user_client.post(
        "/api/jobs/search", json={"parameters": ["Status"], "distinct": True}
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1


def test_user_cannot_submit_parametric_jdl_greater_than_max_parametric_jobs(
    normal_user_client,
):
    """Test that a user cannot submit a parametric JDL greater than the max parametric jobs"""
    job_definitions = [TEST_LARGE_PARAMETRIC_JDL]
    res = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


def test_user_cannot_submit_list_of_jdl_greater_than_max_number_of_jobs(
    normal_user_client,
):
    """Test that a user cannot submit a list of JDL greater than the max number of jobs"""
    job_definitions = [TEST_JDL for _ in range(100)]
    res = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


@pytest.mark.parametrize(
    "job_definitions",
    [[TEST_PARAMETRIC_JDL, TEST_JDL], [TEST_PARAMETRIC_JDL, TEST_PARAMETRIC_JDL]],
)
def test_user_cannot_submit_multiple_jdl_if_at_least_one_of_them_is_parametric(
    normal_user_client, job_definitions
):
    res = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


def test_user_without_the_normal_user_property_cannot_submit_job(admin_user_client):
    res = admin_user_client.post("/api/jobs/", json=[TEST_JDL])
    assert res.status_code == HTTPStatus.FORBIDDEN, res.json()


@pytest.fixture
def valid_job_id(normal_user_client: TestClient):
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    return r.json()[0]["JobID"]


@pytest.fixture
def valid_job_ids(normal_user_client: TestClient):
    job_definitions = [TEST_PARAMETRIC_JDL]
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3
    return sorted([job_dict["JobID"] for job_dict in r.json()])


@pytest.fixture
def invalid_job_id():
    return 999999996


@pytest.fixture
def invalid_job_ids():
    return [999999997, 999999998, 999999999]


def test_get_job_status(normal_user_client: TestClient, valid_job_id: int):
    """Test that the job status is returned correctly."""
    # Act
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")

    # Assert
    assert r.status_code == 200, r.json()
    # TODO: should we return camel case here (and everywhere else) ?
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_get_status_of_nonexistent_job(
    normal_user_client: TestClient, invalid_job_id: int
):
    """Test that the job status is returned correctly."""
    # Act
    r = normal_user_client.get(f"/api/jobs/{invalid_job_id}/status")

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {"detail": f"Job {invalid_job_id} not found"}


def test_get_job_status_in_bulk(normal_user_client: TestClient, valid_job_ids: list):
    """Test that we can get the status of multiple jobs in one request"""
    # Act
    r = normal_user_client.get("/api/jobs/status", params={"job_ids": valid_job_ids})

    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3
    for job_id in valid_job_ids:
        assert str(job_id) in r.json()
        assert r.json()[str(job_id)]["Status"] == JobStatus.SUBMITTING.value
        assert r.json()[str(job_id)]["MinorStatus"] == "Bulk transaction confirmation"
        assert r.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


async def test_get_job_status_history(
    normal_user_client: TestClient, valid_job_id: int
):
    # Arrange
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"

    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    before = datetime.now(timezone.utc)
    r = normal_user_client.patch(
        f"/api/jobs/{valid_job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": NEW_STATUS,
                "MinorStatus": NEW_MINOR_STATUS,
            }
        },
    )
    after = datetime.now(timezone.utc)
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    # Act
    r = normal_user_client.get(
        f"/api/jobs/{valid_job_id}/status/history",
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    assert len(r.json()[str(valid_job_id)]) == 2
    assert r.json()[str(valid_job_id)][0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)][0]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)][0]["ApplicationStatus"] == "Unknown"
    assert r.json()[str(valid_job_id)][0]["Source"] == "JobManager"

    assert r.json()[str(valid_job_id)][1]["Status"] == JobStatus.CHECKING.value
    assert r.json()[str(valid_job_id)][1]["MinorStatus"] == "JobPath"
    assert r.json()[str(valid_job_id)][1]["ApplicationStatus"] == "Unknown"
    assert (
        before
        < datetime.fromisoformat(r.json()[str(valid_job_id)][1]["StatusTime"])
        < after
    )
    assert r.json()[str(valid_job_id)][1]["Source"] == "Unknown"


def test_get_job_status_history_in_bulk(
    normal_user_client: TestClient, valid_job_id: int
):
    # Arrange
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    r = normal_user_client.get(
        "/api/jobs/status/history", params={"job_ids": [valid_job_id]}
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    assert r.json()[str(valid_job_id)][0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)][0]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)][0]["ApplicationStatus"] == "Unknown"
    assert datetime.fromisoformat(r.json()[str(valid_job_id)][0]["StatusTime"])
    assert r.json()[str(valid_job_id)][0]["Source"] == "JobManager"


def test_set_job_status(normal_user_client: TestClient, valid_job_id: int):
    # Arrange
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.patch(
        f"/api/jobs/{valid_job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": NEW_STATUS,
                "MinorStatus": NEW_MINOR_STATUS,
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_invalid_job(
    normal_user_client: TestClient, invalid_job_id: int
):
    # Act
    r = normal_user_client.patch(
        f"/api/jobs/{invalid_job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": JobStatus.CHECKING.value,
                "MinorStatus": "JobPath",
            }
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {"detail": f"Job {invalid_job_id} not found"}


def test_set_job_status_offset_naive_datetime_return_bad_request(
    normal_user_client: TestClient,
    valid_job_id: int,
):
    # Act
    date = datetime.utcnow().isoformat(sep=" ")
    r = normal_user_client.patch(
        f"/api/jobs/{valid_job_id}/status",
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
    normal_user_client: TestClient, valid_job_id: int
):
    # Arrange
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.RUNNING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.patch(
        f"/api/jobs/{valid_job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": NEW_STATUS,
                "MinorStatus": NEW_MINOR_STATUS,
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] != NEW_STATUS
    assert r.json()[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] != NEW_STATUS
    assert r.json()[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_force(normal_user_client: TestClient, valid_job_id: int):
    # Arrange
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Job accepted"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.RUNNING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.patch(
        f"/api/jobs/{valid_job_id}/status",
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
    assert r.json()[str(valid_job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == NEW_STATUS
    assert r.json()[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_bulk(normal_user_client: TestClient, valid_job_ids):
    # Arrange
    for job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{job_id}/status")
        assert r.status_code == 200, r.json()
        assert r.json()[str(job_id)]["Status"] == JobStatus.SUBMITTING.value
        assert r.json()[str(job_id)]["MinorStatus"] == "Bulk transaction confirmation"

    # Act
    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                datetime.now(timezone.utc).isoformat(): {
                    "Status": NEW_STATUS,
                    "MinorStatus": NEW_MINOR_STATUS,
                }
            }
            for job_id in valid_job_ids
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    for job_id in valid_job_ids:
        assert r.json()[str(job_id)]["Status"] == NEW_STATUS
        assert r.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS

        r_get = normal_user_client.get(f"/api/jobs/{job_id}/status")
        assert r_get.status_code == 200, r_get.json()
        assert r_get.json()[str(job_id)]["Status"] == NEW_STATUS
        assert r_get.json()[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS
        assert r_get.json()[str(job_id)]["ApplicationStatus"] == "Unknown"


def test_set_job_status_with_invalid_job_id(
    normal_user_client: TestClient, invalid_job_id: int
):
    # Act
    r = normal_user_client.patch(
        f"/api/jobs/{invalid_job_id}/status",
        json={
            datetime.now(tz=timezone.utc).isoformat(): {
                "Status": JobStatus.CHECKING.value,
                "MinorStatus": "JobPath",
            },
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {"detail": f"Job {invalid_job_id} not found"}


def test_insert_and_reschedule(normal_user_client: TestClient):
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    # Test /jobs/reschedule
    r = normal_user_client.post(
        "/api/jobs/reschedule",
        params={"job_ids": submitted_job_ids},
    )
    assert r.status_code == 200, r.json()


# Test delete job


def test_delete_job_valid_job_id(normal_user_client: TestClient, valid_job_id: int):
    # Act
    r = normal_user_client.delete(f"/api/jobs/{valid_job_id}")

    # Assert
    assert r.status_code == 200, r.json()
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.DELETED
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Checking accounting"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_delete_job_invalid_job_id(normal_user_client: TestClient, invalid_job_id: int):
    # Act
    r = normal_user_client.delete(f"/api/jobs/{invalid_job_id}")

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {"detail": f"Job {invalid_job_id} not found"}


def test_delete_bulk_jobs_valid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int]
):
    # Act
    r = normal_user_client.delete("/api/jobs/", params={"job_ids": valid_job_ids})

    # Assert
    assert r.status_code == 200, r.json()
    for valid_job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
        assert r.status_code == 200, r.json()
        assert r.json()[str(valid_job_id)]["Status"] == JobStatus.DELETED
        assert r.json()[str(valid_job_id)]["MinorStatus"] == "Checking accounting"
        assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_delete_bulk_jobs_invalid_job_ids(
    normal_user_client: TestClient, invalid_job_ids: list[int]
):
    # Act
    r = normal_user_client.delete("/api/jobs/", params={"job_ids": invalid_job_ids})

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {
        "detail": {
            "message": f"Failed to delete {len(invalid_job_ids)} jobs out of {len(invalid_job_ids)}",
            "valid_job_ids": [],
            "failed_job_ids": invalid_job_ids,
        }
    }


def test_delete_bulk_jobs_mix_of_valid_and_invalid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int], invalid_job_ids: list[int]
):
    # Arrange
    job_ids = valid_job_ids + invalid_job_ids

    # Act
    r = normal_user_client.delete("/api/jobs/", params={"job_ids": job_ids})

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {
        "detail": {
            "message": f"Failed to delete {len(invalid_job_ids)} jobs out of {len(job_ids)}",
            "valid_job_ids": valid_job_ids,
            "failed_job_ids": invalid_job_ids,
        }
    }
    for job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{job_id}/status")
        assert r.status_code == 200, r.json()
        assert r.json()[str(job_id)]["Status"] != JobStatus.DELETED


# Test kill job


def test_kill_job_valid_job_id(normal_user_client: TestClient, valid_job_id: int):
    # Act
    r = normal_user_client.post(f"/api/jobs/{valid_job_id}/kill")

    # Assert
    assert r.status_code == 200, r.json()
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == 200, r.json()
    assert r.json()[str(valid_job_id)]["Status"] == JobStatus.KILLED
    assert r.json()[str(valid_job_id)]["MinorStatus"] == "Marked for termination"
    assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_kill_job_invalid_job_id(normal_user_client: TestClient, invalid_job_id: int):
    # Act
    r = normal_user_client.post(f"/api/jobs/{invalid_job_id}/kill")

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {"detail": f"Job {invalid_job_id} not found"}


def test_kill_bulk_jobs_valid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int]
):
    # Act
    r = normal_user_client.post("/api/jobs/kill", params={"job_ids": valid_job_ids})

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json() == valid_job_ids
    for valid_job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
        assert r.status_code == 200, r.json()
        assert r.json()[str(valid_job_id)]["Status"] == JobStatus.KILLED
        assert r.json()[str(valid_job_id)]["MinorStatus"] == "Marked for termination"
        assert r.json()[str(valid_job_id)]["ApplicationStatus"] == "Unknown"


def test_kill_bulk_jobs_invalid_job_ids(
    normal_user_client: TestClient, invalid_job_ids: list[int]
):
    # Act
    r = normal_user_client.post("/api/jobs/kill", params={"job_ids": invalid_job_ids})

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {
        "detail": {
            "message": f"Failed to kill {len(invalid_job_ids)} jobs out of {len(invalid_job_ids)}",
            "valid_job_ids": [],
            "failed_job_ids": invalid_job_ids,
        }
    }


def test_kill_bulk_jobs_mix_of_valid_and_invalid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int], invalid_job_ids: list[int]
):
    # Arrange
    job_ids = valid_job_ids + invalid_job_ids

    # Act
    r = normal_user_client.post("/api/jobs/kill", params={"job_ids": job_ids})

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {
        "detail": {
            "message": f"Failed to kill {len(invalid_job_ids)} jobs out of {len(job_ids)}",
            "valid_job_ids": valid_job_ids,
            "failed_job_ids": invalid_job_ids,
        }
    }
    for valid_job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
        assert r.status_code == 200, r.json()
        # assert the job is not killed
        assert r.json()[str(valid_job_id)]["Status"] != JobStatus.KILLED


# Test remove job


def test_remove_job_valid_job_id(normal_user_client: TestClient, valid_job_id: int):
    # Act
    r = normal_user_client.post(f"/api/jobs/{valid_job_id}/remove")

    # Assert
    assert r.status_code == 200, r.json()
    r = normal_user_client.get(f"/api/jobs/{valid_job_id}/status")
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()


def test_remove_job_invalid_job_id(normal_user_client: TestClient, invalid_job_id: int):
    # Act
    r = normal_user_client.post(f"/api/jobs/{invalid_job_id}/remove")

    # Assert
    assert r.status_code == 200, r.json()


def test_remove_bulk_jobs_valid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int]
):
    # Act
    r = normal_user_client.post("/api/jobs/remove", params={"job_ids": valid_job_ids})

    # Assert
    assert r.status_code == 200, r.json()
    for job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{job_id}/status")
        assert r.status_code == HTTPStatus.NOT_FOUND, r.json()


# Test setting job properties


def test_set_single_job_properties(normal_user_client: TestClient, valid_job_id: int):
    job_id = str(valid_job_id)

    initial_job_state = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": job_id,
                }
            ]
        },
    ).json()[0]

    initial_user_priority = initial_job_state["UserPriority"]
    initial_application_status = initial_job_state["ApplicationStatus"]
    initial_last_update_time = initial_job_state["LastUpdateTime"]

    # Update just one property
    res = normal_user_client.patch(
        f"/api/jobs/{job_id}",
        json={"UserPriority": 2},
    )
    assert res.status_code == 200, res.json()

    new_job_state = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": job_id,
                }
            ]
        },
    ).json()[0]
    new_user_priority = new_job_state["UserPriority"]
    new_application_status = new_job_state["ApplicationStatus"]

    assert initial_application_status == new_application_status
    assert initial_user_priority != new_user_priority
    assert new_user_priority == 2
    assert new_job_state["LastUpdateTime"] == initial_last_update_time

    # Update two properties
    res = normal_user_client.patch(
        f"/api/jobs/{job_id}",
        json={"UserPriority": initial_user_priority, "ApplicationStatus": "Crack"},
        params={"update_timestamp": True},
    )
    assert res.status_code == 200, res.json()

    new_job_state = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": job_id,
                }
            ]
        },
    ).json()[0]
    new_user_priority = new_job_state["UserPriority"]
    new_application_status = new_job_state["ApplicationStatus"]

    assert initial_application_status != new_application_status
    assert new_application_status == "Crack"
    assert initial_user_priority == new_user_priority
    assert new_job_state["LastUpdateTime"] != initial_last_update_time


def test_set_single_job_properties_non_existing_job(
    normal_user_client: TestClient, invalid_job_id: int
):
    job_id = str(invalid_job_id)

    res = normal_user_client.patch(
        f"/api/jobs/{job_id}",
        json={"UserPriority": 2},
    )
    assert res.status_code == HTTPStatus.NOT_FOUND, res.json()


# def test_remove_bulk_jobs_invalid_job_ids(
#     normal_user_client: TestClient, invalid_job_ids: list[int]
# ):
#     # Act
#     r = normal_user_client.post("/api/jobs/remove", params={"job_ids": invalid_job_ids})

#     # Assert
#     assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
#     assert r.json() == {
#         "detail": {
#             "message": f"Failed to remove {len(invalid_job_ids)} jobs out of {len(invalid_job_ids)}",
#             "failed_ids": {
#                 str(invalid_job_id): f"Job {invalid_job_id} not found"
#                 for invalid_job_id in invalid_job_ids
#             },
#         }
#     }


# def test_remove_bulk_jobs_mix_of_valid_and_invalid_job_ids(
#     normal_user_client: TestClient, valid_job_ids: list[int], invalid_job_ids: list[int]
# ):
#     # Arrange
#     job_ids = valid_job_ids + invalid_job_ids

#     # Act
#     r = normal_user_client.post("/api/jobs/remove", params={"job_ids": job_ids})

#     # Assert
#     assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
#     assert r.json() == {
#         "detail": {
#             "message": f"Failed to remove {len(invalid_job_ids)} jobs out of {len(job_ids)}",
#             "failed_ids": {
#                 str(invalid_job_id): f"Job {invalid_job_id} not found"
#                 for invalid_job_id in invalid_job_ids
#             },
#         }
#     }
#     for job_id in valid_job_ids:
#         r = normal_user_client.get(f"/api/jobs/{job_id}/status")
#         assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
