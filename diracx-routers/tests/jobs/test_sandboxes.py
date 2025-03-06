from __future__ import annotations

import hashlib
import secrets
from copy import deepcopy
from io import BytesIO

import httpx
import pytest
from fastapi.testclient import TestClient

from diracx.core.settings import AuthSettings
from diracx.routers.auth.token import create_token

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "JobDB",
        "JobLoggingDB",
        "SandboxMetadataDB",
        "SandboxStoreSettings",
        "WMSAccessPolicy",
        "SandboxAccessPolicy",
        "DevelopmentSettings",
    ]
)


def test_upload_then_download(
    normal_user_client: TestClient, test_auth_settings: AuthSettings
):
    """Test that we can upload a sandbox and then download it."""
    data = secrets.token_bytes(512)
    checksum = hashlib.sha256(data).hexdigest()

    # Initiate the upload
    r = normal_user_client.post(
        "/api/jobs/sandbox",
        json={
            "checksum_algorithm": "sha256",
            "checksum": checksum,
            "size": len(data),
            "format": "tar.bz2",
        },
    )
    assert r.status_code == 200, r.text
    upload_info = r.json()
    assert upload_info["url"]
    sandbox_pfn = upload_info["pfn"]
    assert sandbox_pfn.startswith("SB:SandboxSE|/S3/")

    # Actually upload the file
    files = {"file": ("file", BytesIO(data))}
    r = httpx.post(upload_info["url"], data=upload_info["fields"], files=files)
    assert r.status_code == 204, r.text

    # Make sure we can download it and get the same data back
    r = normal_user_client.get("/api/jobs/sandbox", params={"pfn": sandbox_pfn})
    assert r.status_code == 200, r.text
    download_info = r.json()
    assert download_info["expires_in"] > 5
    r = httpx.get(download_info["url"])
    assert r.status_code == 200, r.text
    assert r.content == data

    # Modify the authorization payload to be another user
    other_user_payload = deepcopy(normal_user_client.dirac_token_payload)
    other_user_payload["preferred_username"] = "other_user"
    other_user_token = create_token(other_user_payload, test_auth_settings)

    # Make sure another user can't download the sandbox
    # The fact that another user cannot download the sandbox
    # is enforced at the policy level, so since in this test
    # we use the AlwaysAllowAccessPolicy, it will actually work !
    r = normal_user_client.get(
        "/api/jobs/sandbox",
        params={"pfn": sandbox_pfn},
        headers={"Authorization": f"Bearer {other_user_token}"},
    )
    assert r.status_code == 200, r.text


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


def test_assign_then_unassign_sandboxes_to_jobs(normal_user_client: TestClient):
    """Test that we can assign and unassign sandboxes to jobs."""
    data = secrets.token_bytes(512)
    checksum = hashlib.sha256(data).hexdigest()

    # Upload Sandbox:
    r = normal_user_client.post(
        "/api/jobs/sandbox",
        json={
            "checksum_algorithm": "sha256",
            "checksum": checksum,
            "size": len(data),
            "format": "tar.bz2",
        },
    )

    assert r.status_code == 200, r.text
    upload_info = r.json()
    assert upload_info["url"]
    sandbox_pfn = upload_info["pfn"]
    assert sandbox_pfn.startswith("SB:SandboxSE|/S3/")

    # Submit a job:
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    job_id = r.json()[0]["JobID"]

    # Getting job input sb:
    r = normal_user_client.get(f"/api/jobs/{job_id}/sandbox/input")
    assert r.status_code == 200
    # Should be empty because
    # (i) JDL doesn't specify any input sb
    # (ii) The sb is not assigned to the job yet
    assert r.json()[0] is None

    # Getting job output sb:
    r = normal_user_client.get(f"/api/jobs/{job_id}/sandbox/output")
    assert r.status_code == 200
    # Should be empty
    assert r.json()[0] is None

    # Assign sb to job:
    r = normal_user_client.patch(
        f"/api/jobs/{job_id}/sandbox/output",
        json=sandbox_pfn,
    )
    assert r.status_code == 200

    # Get the sb again:
    short_pfn = sandbox_pfn.split("|", 1)[-1]
    r = normal_user_client.get(f"/api/jobs/{job_id}/sandbox")
    assert r.status_code == 200
    assert r.json()["Input"] == [None]
    assert r.json()["Output"] == [short_pfn]

    r = normal_user_client.get(f"/api/jobs/{job_id}/sandbox/output")
    assert r.status_code == 200
    assert r.json()[0] == short_pfn

    # Unassign sb to job:
    job_ids = [job_id]
    r = normal_user_client.delete("/api/jobs/sandbox", params={"jobs_ids": job_ids})
    assert r.status_code == 200

    # Get the sb again, it should'nt be there anymore:
    short_pfn = sandbox_pfn.split("|", 1)[-1]
    r = normal_user_client.get(f"/api/jobs/{job_id}/sandbox")
    assert r.status_code == 200
    assert r.json()["Input"] == [None]
    assert r.json()["Output"] == [None]


def test_upload_malformed_checksum(normal_user_client: TestClient):
    """Test that a malformed checksum returns an error."""
    data = secrets.token_bytes(512)
    # Malformed checksum (not a valid sha256)
    checksum = "36_<1P0^Y^OS7SH7P;D<L`>SDV@6`GIUUW^aGEASUKU5dba@KLYVaYDIO3\\=N=KA"

    # Upload Sandbox:
    r = normal_user_client.post(
        "/api/jobs/sandbox",
        json={
            "checksum_algorithm": "sha256",
            "checksum": checksum,
            "size": len(data),
            "format": "tar.bz2",
        },
    )

    assert r.status_code == 422, r.text


def test_upload_oversized(normal_user_client: TestClient):
    """Test that uploading a sandbox that is too large returns an error."""
    data = secrets.token_bytes(512)
    checksum = hashlib.sha256(data).hexdigest()

    # Initiate the upload
    r = normal_user_client.post(
        "/api/jobs/sandbox",
        json={
            "checksum_algorithm": "sha256",
            "checksum": checksum,
            # We can forge the size here to be larger than the actual data as
            # we should get an error and never actually upload the data
            "size": 1024 * 1024 * 1024,
            "format": "tar.bz2",
        },
    )
    assert r.status_code == 400, r.text
    assert "Sandbox too large" in r.json()["detail"], r.text


def test_malformed_request_to_get_job_sandbox(normal_user_client: TestClient):
    """Test that a malformed request to get a job sandbox returns an information to help user."""
    # Submit a job:
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    job_id = r.json()[0]["JobID"]

    # Malformed request:
    r = normal_user_client.get(f"/api/jobs/{job_id}/sandbox/malformed-endpoint")
    assert r.status_code == 422
    assert r.json()["detail"][0]["msg"] == "Input should be 'input' or 'output'"


def test_get_empty_job_sandboxes(normal_user_client: TestClient):
    """Test that we can get the sandboxes of a job that has no sandboxes assigned."""
    # Submit a job:
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    job_id = r.json()[0]["JobID"]

    # Malformed request:
    r = normal_user_client.get(f"/api/jobs/{job_id}/sandbox")
    assert r.status_code == 200
    assert r.json() == {"Input": [None], "Output": [None]}


def test_assign_nonexisting_sb_to_job(normal_user_client: TestClient):
    """Test that we cannot assign a non-existing sandbox to a job."""
    # Submit a job:
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    job_id = r.json()[0]["JobID"]

    # Malformed request:
    r = normal_user_client.patch(
        f"/api/jobs/{job_id}/sandbox/output",
        json="/S3/pathto/vo/vo_group/user/sha256:55967b0c430058c3105472b1edae6c8987c65bcf01ef58f10a3f5e93948782d8.tar.bz2",
    )
    assert r.status_code == 400


def test_assign_sb_to_job_twice(normal_user_client: TestClient):
    """Test that we cannot assign a sandbox to a job twice."""
    data = secrets.token_bytes(512)
    checksum = hashlib.sha256(data).hexdigest()

    # Upload Sandbox:
    r = normal_user_client.post(
        "/api/jobs/sandbox",
        json={
            "checksum_algorithm": "sha256",
            "checksum": checksum,
            "size": len(data),
            "format": "tar.bz2",
        },
    )

    assert r.status_code == 200, r.text
    upload_info = r.json()
    assert upload_info["url"]
    sandbox_pfn = upload_info["pfn"]
    assert sandbox_pfn.startswith("SB:SandboxSE|/S3/")

    # Submit a job:
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/api/jobs/jdl", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    job_id = r.json()[0]["JobID"]

    # Assign sandbox to the job: first attempt should be successful
    r = normal_user_client.patch(
        f"/api/jobs/{job_id}/sandbox/output",
        json=sandbox_pfn,
    )
    assert r.status_code == 200

    # Assign sandbox to the job: second attempt should fail
    r = normal_user_client.patch(
        f"/api/jobs/{job_id}/sandbox/output",
        json=sandbox_pfn,
    )
    assert r.status_code == 400
