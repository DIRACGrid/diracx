from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient
from freezegun import freeze_time

from diracx.core.models import JobStatus

from .conftest import TEST_JDL, TEST_PARAMETRIC_JDL

TEST_LARGE_PARAMETRIC_JDL = """
    Executable = "echo";
    Arguments = "%s";
    JobName = "Test_%n";
    Parameters = 100;
    ParameterStart = 1;
"""

TEST_MALFORMED_JDL = """
[
    'Executable = "echo";'
]
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


def test_insert_malformed_jdl(normal_user_client):
    job_definitions = [TEST_MALFORMED_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 400, r.json()


@freeze_time("2024-01-01T00:00:00.123456Z")
def test_insert_and_search_by_datetime(normal_user_client):
    """Test inserting a job and then searching for it.

    Focus on the SubmissionTime parameter.
    """
    # job_definitions = [TEST_JDL%(normal_user_client.dirac_token_payload)]
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    listed_jobs = r.json()
    assert r.status_code == 200, listed_jobs
    assert len(listed_jobs) == len(job_definitions)

    # 1.1 Search for all jobs submitted in 2024
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 1.2 Search for all jobs submitted before 2024
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "lt",
                    "value": "2024",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 0

    # 2.1 Search for all jobs submitted after 2024-01
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "gt",
                    "value": "2024-01",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 0

    # 2.2 Search for all jobs submitted before 2024-02
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "lt",
                    "value": "2024-02",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 3 Search for all jobs submitted during 2024-01-01
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024-01-01",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 4.1 Search for all jobs submitted during 2024-01-01 00
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024-01-01 00",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 4.2 Search for all jobs submitted during 2024-01-01T00 (with the 'T' separator)
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024-01-01T00",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 4.3 Search for all jobs not submitted during 2024-01-01 01
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "neq",
                    "value": "2024-01-01 01",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 5.1 Search for all jobs submitted after 2024-01-01 00:00:00
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "gt",
                    "value": "2024-01-01 00:00:00",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 0

    # 5.2 Search for all jobs not submitted on 2024-01-01 00:00:00
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "neq",
                    "value": "2024-01-01 00:00:00",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 0

    # 5.3 Search for all jobs submitted on 2024-01-01 00:00:00
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024-01-01 00:00:00",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 6.1 Search for all jobs submitted on 2024-01-01 00:00:00.123456
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024-01-01 00:00:00.123456",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 6.2 Search for all jobs submitted on 2024-01-01 00:00:00.123456Z
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024-01-01 00:00:00.123456Z",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1

    # 6.3 Search for all jobs submitted on 2024-01-01 00:00:00.123Z
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "SubmissionTime",
                    "operator": "eq",
                    "value": "2024-01-01 00:00:00.123Z",
                }
            ]
        },
    )
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1


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

    new_status = JobStatus.CHECKING.value
    new_minor_status = "JobPath"
    before = datetime.now(timezone.utc)

    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                datetime.now(tz=timezone.utc).isoformat(): {
                    "Status": new_status,
                    "MinorStatus": new_minor_status,
                }
            }
        },
    )

    after = datetime.now(timezone.utc)

    assert r.status_code == 200, r.json()
    assert r.json()["success"][str(valid_job_id)]["Status"] == new_status
    assert r.json()["success"][str(valid_job_id)]["MinorStatus"] == new_minor_status

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


@pytest.mark.parametrize(
    "column_name,expected_initial,updated_values",
    [
        ("AccountedFlag", False, [True, False, "Failed", True, "Failed"]),
        # "New"-style jobs are always inserted with VerifiedFlag=True, no idea why...
        ("VerifiedFlag", True, [True, False, True]),
    ],
)
def test_setting_flag(
    normal_user_client: TestClient,
    valid_job_id: int,
    column_name: str,
    expected_initial: bool | str,
    updated_values: list[bool | str],
):
    search_query = {
        "search": [
            {
                "parameter": "JobID",
                "operator": "eq",
                "value": valid_job_id,
            }
        ]
    }

    res = normal_user_client.post("/api/jobs/search", json=search_query)
    assert res.status_code == 200, res.text
    assert len(res.json()) == 1
    assert res.json()[0][column_name] == expected_initial

    for update in updated_values:
        res = normal_user_client.patch(
            "/api/jobs/metadata", json={valid_job_id: {column_name: update}}
        )
        assert res.status_code == 204, res.text

        res = normal_user_client.post("/api/jobs/search", json=search_query)
        assert res.status_code == 200, res.text
        assert len(res.json()) == 1
        assert res.json()[0][column_name] == update


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
