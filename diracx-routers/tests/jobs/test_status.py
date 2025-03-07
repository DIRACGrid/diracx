from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient

from diracx.core.models import JobStatus

from .conftest import TEST_JDL

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
    new_status = JobStatus.CHECKING.value
    new_minor_status = "JobPath"
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

    # Assert
    assert r.status_code == 200, r.json()
    assert r.json()["success"][str(valid_job_id)]["Status"] == new_status
    assert r.json()["success"][str(valid_job_id)]["MinorStatus"] == new_minor_status

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
    assert r.json()[0]["Status"] == new_status
    assert r.json()[0]["MinorStatus"] == new_minor_status
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
    new_status = JobStatus.RUNNING.value
    new_minor_status = "JobPath"
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

    # Assert
    assert r.status_code == 200, r.json()
    success = r.json()["success"]
    assert len(success) == 1, r.json()
    assert success[str(valid_job_id)]["Status"] != new_status
    assert success[str(valid_job_id)]["MinorStatus"] == new_minor_status

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
    assert r.json()[0]["Status"] != new_status
    assert r.json()[0]["MinorStatus"] == new_minor_status
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
    new_status = JobStatus.RUNNING.value
    new_minor_status = "JobPath"
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
        params={"force": True},
    )

    success = r.json()["success"]

    # Assert
    assert r.status_code == 200, r.json()
    assert success[str(valid_job_id)]["Status"] == new_status
    assert success[str(valid_job_id)]["MinorStatus"] == new_minor_status

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
    assert r.json()[0]["Status"] == new_status
    assert r.json()[0]["MinorStatus"] == new_minor_status
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
    new_status = JobStatus.CHECKING.value
    new_minor_status = "JobPath"
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                datetime.now(timezone.utc).isoformat(): {
                    "Status": new_status,
                    "MinorStatus": new_minor_status,
                }
            }
            for job_id in valid_job_ids
        },
    )

    success = r.json()["success"]

    # Assert
    assert r.status_code == 200, r.json()
    for job_id in valid_job_ids:
        assert success[str(job_id)]["Status"] == new_status
        assert success[str(job_id)]["MinorStatus"] == new_minor_status

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
        assert r_get.json()[0]["Status"] == new_status
        assert r_get.json()[0]["MinorStatus"] == new_minor_status
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


## test edge case for rescheduling


def test_reschedule_job_attr_update(normal_user_client: TestClient):
    job_definitions = [TEST_JDL] * 15

    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    # Test /jobs/reschedule and
    # test max_reschedule

    max_resched = 3

    fail_resched_ids = submitted_job_ids[0:5]
    good_resched_ids = list(set(submitted_job_ids) - set(fail_resched_ids))

    for i in range(max_resched):
        r = normal_user_client.post(
            "/api/jobs/reschedule",
            params={"job_ids": fail_resched_ids},
        )
        assert r.status_code == 200, r.json()
        result = r.json()
        successful_results = result["success"]
        for jid in fail_resched_ids:
            assert str(jid) in successful_results, result
            assert successful_results[str(jid)]["Status"] == JobStatus.RECEIVED
            assert successful_results[str(jid)]["MinorStatus"] == "Job Rescheduled"
            assert successful_results[str(jid)]["RescheduleCounter"] == i + 1

    for i in range(max_resched):
        r = normal_user_client.post(
            "/api/jobs/reschedule",
            params={"job_ids": submitted_job_ids},
        )
        assert r.status_code == 200, r.json()
        result = r.json()
        successful_results = result["success"]
        failed_results = result["failed"]
        for jid in good_resched_ids:
            assert str(jid) in successful_results, result
            assert successful_results[str(jid)]["Status"] == JobStatus.RECEIVED
            assert successful_results[str(jid)]["MinorStatus"] == "Job Rescheduled"
            assert successful_results[str(jid)]["RescheduleCounter"] == i + 1
        for jid in fail_resched_ids:
            assert str(jid) in failed_results, result
            # assert successful_results[jid]["Status"] == JobStatus.RECEIVED
            # assert successful_results[jid]["MinorStatus"] == "Job Rescheduled"
            # assert successful_results[jid]["RescheduleCounter"] == i + 1

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
                str(i): {
                    "detail": f"Maximum number of reschedules exceeded ({max_resched})"
                }
                for i in good_resched_ids + fail_resched_ids
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


def test_remove_jobs_valid_job_ids(
    normal_user_client: TestClient, valid_job_ids: list[int]
):
    # Act
    r = normal_user_client.delete("/api/jobs/", params={"job_ids": valid_job_ids})

    # Assert
    assert r.status_code == 200, r.json()
    for job_id in valid_job_ids:
        r = normal_user_client.get(f"/api/jobs/{job_id}/status")
        assert r.status_code == HTTPStatus.NOT_FOUND, r.json()


# def test_remove_jobs_invalid_job_ids(
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


# def test_remove_jobs_mix_of_valid_and_invalid_job_ids(
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


def test_patch_metadata(normal_user_client: TestClient, valid_job_id: int):
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
            ],
            "parameters": ["LoggingInfo"],
        },
    )

    assert r.status_code == 200, r.json()
    for j in r.json():
        assert j["JobID"] == valid_job_id
        assert j["Status"] == JobStatus.RECEIVED.value
        assert j["MinorStatus"] == "Job accepted"
        assert j["ApplicationStatus"] == "Unknown"

    # Act
    hbt = str(datetime.now(timezone.utc))
    r = normal_user_client.patch(
        "/api/jobs/metadata",
        json={
            valid_job_id: {
                "UserPriority": 2,
                "HeartBeatTime": hbt,
                # set a parameter
                "JobType": "VerySpecialIndeed",
            }
        },
    )

    # Assert
    assert (
        r.status_code == 204
    ), "PATCH metadata should return 204 No Content on success"
    r = normal_user_client.post(
        "/api/jobs/search",
        json={
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "eq",
                    "value": valid_job_id,
                }
            ],
            "parameters": ["LoggingInfo"],
        },
    )
    assert r.status_code == 200, r.json()

    assert r.json()[0]["JobID"] == valid_job_id
    assert r.json()[0]["JobType"] == "VerySpecialIndeed"
    assert datetime.fromisoformat(
        r.json()[0]["HeartBeatTime"]
    ) == datetime.fromisoformat(hbt)
    assert r.json()[0]["UserPriority"] == 2


def test_bad_patch_metadata(normal_user_client: TestClient, valid_job_id: int):
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
            ],
            "parameters": ["LoggingInfo"],
        },
    )

    assert r.status_code == 200, r.json()
    for j in r.json():
        assert j["JobID"] == valid_job_id
        assert j["Status"] == JobStatus.RECEIVED.value
        assert j["MinorStatus"] == "Job accepted"
        assert j["ApplicationStatus"] == "Unknown"

    # Act
    hbt = str(datetime.now(timezone.utc))
    r = normal_user_client.patch(
        "/api/jobs/metadata",
        json={
            valid_job_id: {
                "UserPriority": 2,
                "Heartbeattime": hbt,
                # set a parameter
                "JobType": "VerySpecialIndeed",
            }
        },
    )

    # Assert
    assert (
        r.status_code == 400
    ), "PATCH metadata should 400 Bad Request if an attribute column's case is incorrect"
