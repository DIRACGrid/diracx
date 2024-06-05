from __future__ import annotations

import hashlib
import secrets
from copy import deepcopy
from io import BytesIO

import pytest
import requests
from fastapi.testclient import TestClient

from diracx.routers.auth.token import create_token
from diracx.routers.utils.users import AuthSettings

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "JobDB",
        "JobLoggingDB",
        "SandboxMetadataDB",
        "SandboxStoreSettings",
        "WMSAccessPolicy",
        "SandboxAccessPolicy",
    ]
)


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


def test_upload_then_download(
    normal_user_client: TestClient, test_auth_settings: AuthSettings
):
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
    r = requests.post(upload_info["url"], data=upload_info["fields"], files=files)
    assert r.status_code == 204, r.text

    # Make sure we can download it and get the same data back
    r = normal_user_client.get("/api/jobs/sandbox", params={"pfn": sandbox_pfn})
    assert r.status_code == 200, r.text
    download_info = r.json()
    assert download_info["expires_in"] > 5
    r = requests.get(download_info["url"])
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


def test_upload_oversized(normal_user_client: TestClient):
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
    r = normal_user_client.post("/api/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    job_id = r.json()[0]["JobID"]

    # Getting job sb:
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
