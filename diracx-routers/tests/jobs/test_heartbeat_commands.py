from __future__ import annotations

from datetime import datetime, timedelta, timezone
from time import sleep

import pytest
from fastapi.testclient import TestClient

from diracx.core.models.job import JobStatus

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


def test_heartbeat(normal_user_client: TestClient, valid_job_id: int):
    search_body = {
        "search": [{"parameter": "JobID", "operator": "eq", "value": valid_job_id}]
    }
    r = normal_user_client.post("/api/jobs/search", json=search_body)
    r.raise_for_status()
    old_data = r.json()[0]
    assert old_data["HeartBeatTime"] is None

    payload = {valid_job_id: {"Vsize": 1234}}
    r = normal_user_client.patch("/api/jobs/heartbeat", json=payload)
    r.raise_for_status()

    r = normal_user_client.post("/api/jobs/search", json=search_body)
    r.raise_for_status()
    new_data = r.json()[0]

    hbt = datetime.fromisoformat(new_data["HeartBeatTime"])
    # This should be timezone aware due to the enforced tzinfo from
    # the SQLAlchemy type used for datetime fields in JobDB
    assert hbt.tzinfo is not None
    assert hbt >= datetime.now(tz=timezone.utc) - timedelta(seconds=15)

    # Kill the job by setting the status on it
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
    r.raise_for_status()

    sleep(1)
    # Send another heartbeat and check that a Kill job command was set
    payload = {valid_job_id: {"Vsize": 1235}}
    r = normal_user_client.patch("/api/jobs/heartbeat", json=payload)
    r.raise_for_status()

    commands = r.json()
    assert len(commands) == 1, "Exactly one job command should be returned"
    assert commands[0]["job_id"] == valid_job_id, (
        f"Wrong job id, should be '{valid_job_id}' but got {commands[0]['job_id']=}"
    )
    assert commands[0]["command"] == "Kill", (
        f"Wrong job command received, should be 'Kill' but got {commands[0]=}"
    )
    sleep(1)

    # Send another heartbeat and check the job commands are empty
    payload = {valid_job_id: {"Vsize": 1234}}
    r = normal_user_client.patch("/api/jobs/heartbeat", json=payload)
    r.raise_for_status()
    commands = r.json()
    assert len(commands) == 0, (
        "Exactly zero job commands should be returned after heartbeat commands are sent"
    )


def test_multiple_jobs_receive_independent_kill_commands(
    normal_user_client: TestClient,
    valid_job_ids: list[int],
):
    """Verify that multiple jobs each receive exactly one Kill command."""
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            job_id: {
                datetime.now(timezone.utc).isoformat(): {
                    "Status": JobStatus.KILLED,
                    "MinorStatus": "Marked for termination",
                }
            }
            for job_id in valid_job_ids
        },
    )
    r.raise_for_status()

    sleep(1)

    r = normal_user_client.patch(
        "/api/jobs/heartbeat",
        json={job_id: {"Vsize": 2000} for job_id in valid_job_ids},
    )
    r.raise_for_status()

    commands = r.json()
    assert len(commands) == len(valid_job_ids)
    assert {cmd["job_id"] for cmd in commands} == set(valid_job_ids)
    assert {cmd["command"] for cmd in commands} == {"Kill"}

    sleep(1)

    r = normal_user_client.patch(
        "/api/jobs/heartbeat",
        json={job_id: {"Vsize": 2001} for job_id in valid_job_ids},
    )
    r.raise_for_status()

    assert r.json() == []


def test_non_killed_status_does_not_create_command(
    normal_user_client: TestClient,
    valid_job_id: int,
):
    """Verify statuses different from KILLED do not enqueue job commands."""
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                datetime.now(timezone.utc).isoformat(): {
                    "Status": JobStatus.RUNNING,
                    "MinorStatus": "Normal transition",
                }
            }
        },
    )
    r.raise_for_status()

    sleep(1)

    r = normal_user_client.patch(
        "/api/jobs/heartbeat",
        json={valid_job_id: {"Vsize": 500}},
    )
    r.raise_for_status()

    assert r.json() == []


def test_deleted_creates_kill_command(
    normal_user_client: TestClient,
    valid_job_id: int,
):
    """Verify DELETED follows the same command path as KILLED."""
    r = normal_user_client.patch(
        "/api/jobs/status",
        json={
            valid_job_id: {
                datetime.now(timezone.utc).isoformat(): {
                    "Status": JobStatus.DELETED,
                    "MinorStatus": "User removed job",
                }
            }
        },
    )
    r.raise_for_status()

    sleep(1)

    r = normal_user_client.patch(
        "/api/jobs/heartbeat",
        json={valid_job_id: {"Vsize": 123}},
    )
    r.raise_for_status()

    commands = r.json()
    assert len(commands) == 1
    assert commands[0]["job_id"] == valid_job_id
    assert commands[0]["command"] == "Kill"

    sleep(1)

    r = normal_user_client.patch(
        "/api/jobs/heartbeat",
        json={valid_job_id: {"Vsize": 124}},
    )
    r.raise_for_status()

    assert r.json() == []
