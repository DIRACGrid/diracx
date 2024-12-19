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

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "JobDB",
        "JobLoggingDB",
        "ConfigSource",
        "TaskQueueDB",
        "SandboxMetadataDB",
        "WMSAccessPolicy",
        "DevelopmentSettings",
        "JobParametersDB",
    ]
)


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


@pytest.fixture
def admin_user_client(client_factory):
    with client_factory.admin_user() as client:
        yield client


def test_insert_and_list_parametric_jobs(normal_user_client):
    job_definitions = [TEST_PARAMETRIC_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.post("/api/jobs/search")
    assert r.status_code == 200, r.json()

    listed_jobs = r.json()

    assert "Content-Range" not in r.headers

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
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.post("/api/jobs/search")
    assert r.status_code == 200, r.json()

    listed_jobs = r.json()

    assert "Content-Range" not in r.headers

    assert len(listed_jobs) == len(job_definitions)

    assert submitted_job_ids == sorted([job_dict["JobID"] for job_dict in listed_jobs])


def test_insert_and_search(normal_user_client):
    """Test inserting a job and then searching for it."""
    # job_definitions = [TEST_JDL%(normal_user_client.dirac_token_payload)]
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert len(listed_jobs) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    # Test /jobs/search
    # 1. Search for all jobs
    r = normal_user_client.post("/api/jobs/search")
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert [x["JobID"] for x in listed_jobs] == submitted_job_ids
    assert {x["VerifiedFlag"] for x in listed_jobs} == {True}

    # 2. Search for all jobs with status NEW: should return an empty list
    r = normal_user_client.post(
        "/api/jobs/search",
        json={"search": [{"parameter": "Status", "operator": "eq", "value": "NEW"}]},
    )
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert listed_jobs == []

    assert "Content-Range" not in r.headers

    # 3. Search for all jobs with status RECEIVED: should return the submitted jobs
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
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert [x["JobID"] for x in listed_jobs] == submitted_job_ids

    assert "Content-Range" not in r.headers

    # 4. Search for all jobs but just return the JobID and the Status
    r = normal_user_client.post(
        "/api/jobs/search", json={"parameters": ["JobID", "Status"]}
    )
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert listed_jobs == [
        {"JobID": jid, "Status": JobStatus.RECEIVED.value} for jid in submitted_job_ids
    ]

    assert "Content-Range" not in r.headers

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
    """Test that the distinct parameter works as expected."""
    job_definitions = [TEST_JDL, TEST_JDL, TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert len(listed_jobs) == len(job_definitions)

    # Check that distinct collapses identical records when true
    r = normal_user_client.post(
        "/api/jobs/search", json={"parameters": ["Status"], "distinct": False}
    )
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert len(listed_jobs) > 1

    assert "Content-Range" not in r.headers

    r = normal_user_client.post(
        "/api/jobs/search", json={"parameters": ["Status"], "distinct": True}
    )
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert len(listed_jobs) == 1

    assert "Content-Range" not in r.headers


def test_search_pagination(normal_user_client):
    """Test that the pagination works as expected."""
    job_definitions = [TEST_JDL] * 20
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert len(listed_jobs) == len(job_definitions)

    # Get the first 20 jobs (all of them)
    r = normal_user_client.post("/api/jobs/search", params={"page": 1, "per_page": 20})
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert len(listed_jobs) == 20

    assert "Content-Range" not in r.headers

    # Get the first 10 jobs
    r = normal_user_client.post("/api/jobs/search", params={"page": 1, "per_page": 10})
    listed_jobs = r.json()
    assert r.status_code == 206, listed_jobs
    assert len(listed_jobs) == 10

    assert "Content-Range" in r.headers
    assert (
        r.headers["Content-Range"]
        == f"jobs 0-{len(listed_jobs) -1}/{len(job_definitions)}"
    )

    # Get the next 10 jobs
    r = normal_user_client.post("/api/jobs/search", params={"page": 2, "per_page": 10})
    listed_jobs = r.json()
    assert r.status_code == 206, listed_jobs
    assert len(listed_jobs) == 10

    assert "Content-Range" in r.headers
    assert (
        r.headers["Content-Range"]
        == f"jobs 10-{len(listed_jobs) + 10 - 1}/{len(job_definitions)}"
    )

    # Get an unknown page
    r = normal_user_client.post("/api/jobs/search", params={"page": 3, "per_page": 10})
    listed_jobs = r.json()
    assert r.status_code == 416, listed_jobs
    assert len(listed_jobs) == 0

    assert "Content-Range" in r.headers
    assert r.headers["Content-Range"] == f"jobs */{len(job_definitions)}"

    # Set the per_page parameter to 0
    r = normal_user_client.post("/api/jobs/search", params={"page": 1, "per_page": 0})
    assert r.status_code == 400, r.json()

    # Set the per_page parameter to a negative number
    r = normal_user_client.post("/api/jobs/search", params={"page": 1, "per_page": -1})
    assert r.status_code == 400, r.json()

    # Set the page parameter to 0
    r = normal_user_client.post("/api/jobs/search", params={"page": 0, "per_page": 10})
    assert r.status_code == 400, r.json()

    # Set the page parameter to a negative number
    r = normal_user_client.post("/api/jobs/search", params={"page": -1, "per_page": 10})
    assert r.status_code == 400, r.json()


def test_user_cannot_submit_parametric_jdl_greater_than_max_parametric_jobs(
    normal_user_client,
):
    """Test that a user cannot submit a parametric JDL greater than the max parametric jobs."""
    job_definitions = [TEST_LARGE_PARAMETRIC_JDL]
    res = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


def test_user_cannot_submit_list_of_jdl_greater_than_max_number_of_jobs(
    normal_user_client,
):
    """Test that a user cannot submit a list of JDL greater than the max number of jobs."""
    job_definitions = [TEST_JDL for _ in range(100)]
    res = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


@pytest.mark.parametrize(
    "job_definitions",
    [[TEST_PARAMETRIC_JDL, TEST_JDL], [TEST_PARAMETRIC_JDL, TEST_PARAMETRIC_JDL]],
)
def test_user_cannot_submit_multiple_jdl_if_at_least_one_of_them_is_parametric(
    normal_user_client, job_definitions
):
    res = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert res.status_code == HTTPStatus.BAD_REQUEST, res.json()


def test_user_without_the_normal_user_property_cannot_submit_job(admin_user_client):
    pytest.skip(
        "AlwaysAllowAccessPolicyCallable is forced in testing, so this test can not actually test this access policy."
    )
    res = admin_user_client.post("/api/jobs/jdl", json=[TEST_JDL])
    assert res.status_code == HTTPStatus.FORBIDDEN, res.json()


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


def test_get_job_status(normal_user_client: TestClient, valid_job_id: int):
    """Test that the job status is returned correctly."""
    # Act
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "parameters": ["JobID", "Status", "MinorStatus", "ApplicationStatus"],
            "search": [{"parameter": "JobID", "operator": "eq", "value": valid_job_id}],
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1, f"Should only return length-1 list: {r.json()}"
    assert r.json()[0]["JobID"] == valid_job_id, "Returned wrong job id"
    # TODO: should we return camel case here (and everywhere else) ?
    assert r.json()[0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[0]["MinorStatus"] == "Job accepted"
    assert r.json()[0]["ApplicationStatus"] == "Unknown"


def test_get_status_of_nonexistent_job(
    normal_user_client: TestClient, invalid_job_id: int
):
    """Test that the job status is returned correctly."""
    # Act
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "parameters": ["Status"],
            "search": [
                {"parameter": "JobID", "operator": "eq", "value": invalid_job_id}
            ],
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.OK, r.json()
    assert r.json() == []


def test_get_job_status_in_bulk(normal_user_client: TestClient, valid_job_ids: list):
    """Test that we can get the status of multiple jobs in one request."""
    # Act

    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "parameters": ["JobID", "Status", "MinorStatus", "ApplicationStatus"],
            "search": [
                {"parameter": "JobID", "operator": "in", "values": valid_job_ids}
            ],
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3
    assert {j["JobID"] for j in r.json()} == set(valid_job_ids)
    for job in r.json():
        assert job["JobID"] in valid_job_ids
        assert job["Status"] == JobStatus.SUBMITTING.value
        assert job["MinorStatus"] == "Bulk transaction confirmation"
        assert job["ApplicationStatus"] == "Unknown"


async def test_get_job_status_history(
    normal_user_client: TestClient, valid_job_id: int
):
    # Arrange
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "parameters": ["JobID", "Status", "MinorStatus", "ApplicationStatus"],
            "search": [{"parameter": "JobID", "operator": "eq", "value": valid_job_id}],
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json()[0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[0]["MinorStatus"] == "Job accepted"
    assert r.json()[0]["ApplicationStatus"] == "Unknown"

    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    before = datetime.now(timezone.utc)

    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                datetime.now(tz=timezone.utc).isoformat(): {
                    "Status": NEW_STATUS,
                    "MinorStatus": NEW_MINOR_STATUS,
                }
            }
        },
    )

    after = datetime.now(timezone.utc)

    assert r.status_code == 200, r.json()
    assert r.json()["success"][str(valid_job_id)]["Status"] == NEW_STATUS
    assert r.json()["success"][str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    # Act
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "parameters": [
                "JobID",
                "Status",
                "MinorStatus",
                "ApplicationStatus",
                "LoggingInfo",
            ],
            "search": [{"parameter": "JobID", "operator": "eq", "value": valid_job_id}],
        },
    )
    # Assert
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    assert len(r.json()[0]["LoggingInfo"]) == 2
    assert r.json()[0]["LoggingInfo"][0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[0]["LoggingInfo"][0]["MinorStatus"] == "Job accepted"
    assert r.json()[0]["LoggingInfo"][0]["ApplicationStatus"] == "Unknown"
    assert r.json()[0]["LoggingInfo"][0]["Source"] == "JobManager"

    assert r.json()[0]["LoggingInfo"][1]["Status"] == JobStatus.CHECKING.value
    assert r.json()[0]["LoggingInfo"][1]["MinorStatus"] == "JobPath"
    assert r.json()[0]["LoggingInfo"][1]["ApplicationStatus"] == "Unknown"
    assert (
        before
        < datetime.fromisoformat(r.json()[0]["LoggingInfo"][1]["StatusTime"])
        < after
    )
    assert r.json()[0]["LoggingInfo"][1]["Source"] == "Unknown"


def test_get_job_status_history_in_bulk(
    normal_user_client: TestClient, valid_job_id: int
):
    pytest.skip("TODO: decide whether to keep this")

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
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )

    assert r.status_code == 200, r.json()
    for j in r.json():
        assert j["JobID"] == valid_job_id
        assert j["Status"] == JobStatus.RECEIVED.value
        assert j["MinorStatus"] == "Job accepted"
        assert j["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.CHECKING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                datetime.now(tz=timezone.utc).isoformat(): {
                    "Status": NEW_STATUS,
                    "MinorStatus": NEW_MINOR_STATUS,
                }
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json()["success"][str(valid_job_id)]["Status"] == NEW_STATUS
    assert r.json()["success"][str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json()[0]["JobID"] == valid_job_id
    assert r.json()[0]["Status"] == NEW_STATUS
    assert r.json()[0]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[0]["ApplicationStatus"] == "Unknown"


def test_set_job_status_invalid_job(
    normal_user_client: TestClient, invalid_job_id: int
):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            invalid_job_id: {
                datetime.now(tz=timezone.utc).isoformat(): {
                    "Status": JobStatus.CHECKING.value,
                    "MinorStatus": "JobPath",
                }
            }
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {
        "detail": {
            "success": {},
            "failed": {str(invalid_job_id): {"detail": "Not found"}},
        }
    }


def test_set_job_status_offset_naive_datetime_return_bad_request(
    normal_user_client: TestClient,
    valid_job_id: int,
):
    # Act
    date = datetime.now(tz=timezone.utc).isoformat(sep=" ").split("+")[0]
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                date: {
                    "Status": JobStatus.CHECKING.value,
                    "MinorStatus": "JobPath",
                }
            }
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.BAD_REQUEST, r.json()
    assert r.json() == {
        "detail": f"Timestamp {date} is not timezone aware for job {valid_job_id}"
    }


def test_set_job_status_cannot_make_impossible_transitions(
    normal_user_client: TestClient, valid_job_id: int
):
    # Arrange
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json()[0]["JobID"] == valid_job_id
    assert r.json()[0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[0]["MinorStatus"] == "Job accepted"
    assert r.json()[0]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.RUNNING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                datetime.now(tz=timezone.utc).isoformat(): {
                    "Status": NEW_STATUS,
                    "MinorStatus": NEW_MINOR_STATUS,
                }
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    success = r.json()["success"]
    assert len(success) == 1, r.json()
    assert success[str(valid_job_id)]["Status"] != NEW_STATUS
    assert success[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json()[0]["Status"] != NEW_STATUS
    assert r.json()[0]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[0]["ApplicationStatus"] == "Unknown"


def test_set_job_status_force(normal_user_client: TestClient, valid_job_id: int):
    # Arrange
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json()[0]["JobID"] == valid_job_id
    assert r.json()[0]["Status"] == JobStatus.RECEIVED.value
    assert r.json()[0]["MinorStatus"] == "Job accepted"
    assert r.json()[0]["ApplicationStatus"] == "Unknown"

    # Act
    NEW_STATUS = JobStatus.RUNNING.value
    NEW_MINOR_STATUS = "JobPath"
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                datetime.now(tz=timezone.utc).isoformat(): {
                    "Status": NEW_STATUS,
                    "MinorStatus": NEW_MINOR_STATUS,
                }
            }
        },
        params={"force": True},
    )

    success = r.json()["success"]

    # Assert
    assert r.status_code == 200, r.json()
    assert success[str(valid_job_id)]["Status"] == NEW_STATUS
    assert success[str(valid_job_id)]["MinorStatus"] == NEW_MINOR_STATUS

    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json()[0]["JobID"] == valid_job_id
    assert r.json()[0]["Status"] == NEW_STATUS
    assert r.json()[0]["MinorStatus"] == NEW_MINOR_STATUS
    assert r.json()[0]["ApplicationStatus"] == "Unknown"


def test_set_job_status_bulk(normal_user_client: TestClient, valid_job_ids):
    # Arrange
    for job_id in valid_job_ids:
        r = normal_user_client.post(
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
        )
        assert r.status_code == 200, r.json()
        assert r.json()[0]["JobID"] == job_id
        assert r.json()[0]["Status"] == JobStatus.SUBMITTING.value
        assert r.json()[0]["MinorStatus"] == "Bulk transaction confirmation"

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

    success = r.json()["success"]

    # Assert
    assert r.status_code == 200, r.json()
    for job_id in valid_job_ids:
        assert success[str(job_id)]["Status"] == NEW_STATUS
        assert success[str(job_id)]["MinorStatus"] == NEW_MINOR_STATUS

        r_get = normal_user_client.post(
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
        )
        assert r_get.status_code == 200, r_get.json()
        assert r_get.json()[0]["JobID"] == job_id
        assert r_get.json()[0]["Status"] == NEW_STATUS
        assert r_get.json()[0]["MinorStatus"] == NEW_MINOR_STATUS
        assert r_get.json()[0]["ApplicationStatus"] == "Unknown"


def test_set_job_status_with_invalid_job_id(
    normal_user_client: TestClient, invalid_job_id: int
):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            invalid_job_id: {
                datetime.now(tz=timezone.utc).isoformat(): {
                    "Status": JobStatus.CHECKING.value,
                    "MinorStatus": "JobPath",
                }
            },
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json()["detail"] == {
        "success": {},
        "failed": {str(invalid_job_id): {"detail": "Not found"}},
    }


def test_insert_and_reschedule(normal_user_client: TestClient):
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    # Test /jobs/reschedule and
    # test max_reschedule

    max_resched = 3
    jid = str(submitted_job_ids[0])

    for i in range(max_resched):
        r = normal_user_client.post(
            "/api/jobs/reschedule",
            params={"job_ids": submitted_job_ids},
        )
        assert r.status_code == 200, r.json()
        result = r.json()
        successful_results = result["success"]
        assert jid in successful_results, result
        assert successful_results[jid]["Status"] == JobStatus.RECEIVED
        assert successful_results[jid]["MinorStatus"] == "Job Rescheduled"
        assert successful_results[jid]["RescheduleCounter"] == i + 1

    r = normal_user_client.post(
        "/api/jobs/reschedule",
        params={"job_ids": submitted_job_ids},
    )
    assert (
        r.status_code != 200
    ), f"Rescheduling more than {max_resched} times should have failed by now {r.json()}"
    assert r.json() == {
        "detail": {
            "success": [],
            "failed": {
                "1": {
                    "detail": f"Maximum number of reschedules exceeded ({max_resched})"
                }
            },
        }
    }


# Test delete job


def test_delete_job_valid_job_id(normal_user_client: TestClient, valid_job_id: int):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                str(datetime.now(tz=timezone.utc)): {
                    "Status": JobStatus.DELETED,
                    "MinorStatus": "Checking accounting",
                }
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert r.json()[0]["JobID"] == valid_job_id
    assert r.json()[0]["Status"] == JobStatus.DELETED
    assert r.json()[0]["MinorStatus"] == "Checking accounting"
    assert r.json()[0]["ApplicationStatus"] == "Unknown"


def test_delete_job_invalid_job_id(normal_user_client: TestClient, invalid_job_id: int):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            invalid_job_id: {
                str(datetime.now(tz=timezone.utc)): {
                    "Status": JobStatus.DELETED,
                    "MinorStatus": "Checking accounting",
                }
            }
        },
    )
    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json()["detail"]["failed"] == {
        str(invalid_job_id): {"detail": "Not found"}
    }


def test_delete_bulk_jobs_valid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int]
):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                str(datetime.now(tz=timezone.utc)): {
                    "Status": JobStatus.DELETED,
                    "MinorStatus": "Checking accounting",
                }
            }
            for job_id in valid_job_ids
        },
    )
    req = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "in",
                    "values": valid_job_ids,
                }
            ]
        },
    )
    assert req.status_code == 200, req.json()

    r = {i["JobID"]: i for i in req.json()}
    for valid_job_id in valid_job_ids:
        assert r[valid_job_id]["Status"] == JobStatus.DELETED
        assert r[valid_job_id]["MinorStatus"] == "Checking accounting"
        assert r[valid_job_id]["ApplicationStatus"] == "Unknown"


def test_delete_bulk_jobs_invalid_job_ids(
    normal_user_client: TestClient, invalid_job_ids: list[int]
):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                str(datetime.now(tz=timezone.utc)): {
                    "Status": JobStatus.DELETED,
                    "MinorStatus": "Checking accounting",
                }
            }
            for job_id in invalid_job_ids
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json() == {
        "detail": {
            "success": {},
            "failed": {str(jid): {"detail": "Not found"} for jid in invalid_job_ids},
        }
    }


def test_delete_bulk_jobs_mix_of_valid_and_invalid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int], invalid_job_ids: list[int]
):
    # Arrange
    job_ids = valid_job_ids + invalid_job_ids

    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                str(datetime.now(tz=timezone.utc)): {
                    "Status": JobStatus.DELETED,
                    "MinorStatus": "Checking accounting",
                }
            }
            for job_id in job_ids
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.OK, r.json()
    resp = r.json()

    assert len(resp["success"]) == len(valid_job_ids)
    assert resp["failed"] == {
        "999999997": {"detail": "Not found"},
        "999999998": {"detail": "Not found"},
        "999999999": {"detail": "Not found"},
    }

    req = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "in",
                    "values": valid_job_ids,
                }
            ]
        },
    )
    assert req.status_code == 200, req.json()

    r = req.json()
    assert len(r) == len(valid_job_ids), r
    for job in r:
        assert job["Status"] == JobStatus.DELETED
        assert job["MinorStatus"] == "Checking accounting"


# Test kill job
def test_kill_job_valid_job_id(normal_user_client: TestClient, valid_job_id: int):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                str(datetime.now(timezone.utc)): {
                    "Status": JobStatus.KILLED,
                    "MinorStatus": "Marked for termination",
                }
            }
        },
    )

    # Assert
    assert r.status_code == 200, r.json()

    successful = r.json()["success"]
    assert successful[str(valid_job_id)]["Status"] == JobStatus.KILLED
    req = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert req.status_code == 200, successful
    assert req.json()[0]["JobID"] == valid_job_id
    assert req.json()[0]["Status"] == JobStatus.KILLED
    assert req.json()[0]["MinorStatus"] == "Marked for termination"
    assert req.json()[0]["ApplicationStatus"] == "Unknown"


def test_kill_job_invalid_job_id(normal_user_client: TestClient, invalid_job_id: int):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            int(invalid_job_id): {
                str(datetime.now(timezone.utc)): {
                    "Status": JobStatus.KILLED,
                    "MinorStatus": "Marked for termination",
                }
            }
        },
    )

    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()
    assert r.json()["detail"] == {
        "success": {},
        "failed": {str(invalid_job_id): {"detail": "Not found"}},
    }


def test_kill_bulk_jobs_valid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int]
):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                str(datetime.now(timezone.utc)): {
                    "Status": JobStatus.KILLED,
                    "MinorStatus": "Marked for termination",
                }
            }
            for job_id in valid_job_ids
        },
    )
    result = r.json()

    # Assert
    assert r.status_code == 200, result
    req = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "in",
                    "values": valid_job_ids,
                }
            ]
        },
    )
    assert req.status_code == 200, req.json()

    r = req.json()
    assert len(r) == len(valid_job_ids), r
    for job in r:
        assert job["Status"] == JobStatus.KILLED
        assert job["MinorStatus"] == "Marked for termination"
        assert job["ApplicationStatus"] == "Unknown"


def test_kill_bulk_jobs_invalid_job_ids(
    normal_user_client: TestClient, invalid_job_ids: list[int]
):
    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                str(datetime.now(timezone.utc)): {
                    "Status": JobStatus.KILLED,
                    "MinorStatus": "Marked for termination",
                }
            }
            for job_id in invalid_job_ids
        },
    )
    # Assert
    assert r.status_code == HTTPStatus.NOT_FOUND, r.json()

    assert r.json()["detail"] == {
        "success": {},
        "failed": {
            "999999997": {"detail": "Not found"},
            "999999998": {"detail": "Not found"},
            "999999999": {"detail": "Not found"},
        },
    }


def test_kill_bulk_jobs_mix_of_valid_and_invalid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int], invalid_job_ids: list[int]
):
    # Arrange
    job_ids = valid_job_ids + invalid_job_ids

    # Act
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                str(datetime.now(timezone.utc)): {
                    "Status": JobStatus.KILLED,
                    "MinorStatus": "Marked for termination",
                }
            }
            for job_id in job_ids
        },
    )
    # Assert
    assert r.status_code == HTTPStatus.OK, r.json()
    resp = r.json()

    assert len(resp["success"]) == len(valid_job_ids)
    assert resp["failed"] == {
        "999999997": {"detail": "Not found"},
        "999999998": {"detail": "Not found"},
        "999999999": {"detail": "Not found"},
    }

    req = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "in",
                    "values": valid_job_ids,
                }
            ]
        },
    )
    assert req.status_code == 200, req.json()

    r = req.json()
    assert len(r) == len(valid_job_ids), r
    for job in r:
        assert job["Status"] == JobStatus.KILLED
        assert job["MinorStatus"] == "Marked for termination"
        assert job["ApplicationStatus"] == "Unknown"


# Test remove job


def test_remove_job_valid_job_id(normal_user_client: TestClient, valid_job_id: int):
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == HTTPStatus.OK, r.json()
    assert r.json() != []

    # Act
    r = normal_user_client.delete(
        "/api/jobs/",
        params={
            "job_ids": [valid_job_id],
        },
    )

    assert r.status_code == HTTPStatus.OK, r.json()

    # Assert
    assert r.status_code == 200, r.json()
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ]
        },
    )
    assert r.status_code == HTTPStatus.OK, r.json()
    assert r.json() == []


def test_remove_job_invalid_job_id(normal_user_client: TestClient, invalid_job_id: int):
    # Act
    r = normal_user_client.delete(
        "/api/jobs/",
        params={
            "job_ids": [invalid_job_id],
        },
    )

    # Assert
    assert r.status_code == 200, r.json()


def test_remove_bulk_jobs_valid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int]
):
    # Act
    r = normal_user_client.delete("/api/jobs/", params={"job_ids": valid_job_ids})

    # Assert
    assert r.status_code == 200, r.json()
    for job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{job_id}/status")
        assert r.status_code == HTTPStatus.NOT_FOUND, r.json()


# Test setting job properties


def test_set_single_job_properties(normal_user_client: TestClient, valid_job_id: int):
    pytest.skip("There seems to be a missing route for this - TODO")

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
        "/api/jobs/",
        json={valid_job_id: {"UserPriority": 2}},
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
    pytest.skip("There seems to be a missing route for this - TODO")
    job_id = str(invalid_job_id)

    res = normal_user_client.patch(
        "/api/jobs/",
        json={job_id: {"UserPriority": 2}},
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
