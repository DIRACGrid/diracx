from __future__ import annotations

import hashlib
import secrets
from copy import deepcopy
from io import BytesIO

import requests
from fastapi.testclient import TestClient

from diracx.routers.auth import AuthSettings, create_token


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
    r = normal_user_client.get(
        "/api/jobs/sandbox",
        params={"pfn": sandbox_pfn},
        headers={"Authorization": f"Bearer {other_user_token}"},
    )
    assert r.status_code == 400, r.text
    assert "Invalid PFN. PFN must start with" in r.json()["detail"]


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
