from __future__ import annotations

import hashlib
import secrets
from datetime import timedelta
from io import BytesIO
from typing import AsyncGenerator, Generator

import botocore.exceptions
import freezegun
import httpx
import pytest
import sqlalchemy

from diracx.core.exceptions import SandboxNotFoundError
from diracx.core.models import ChecksumAlgorithm, SandboxFormat, SandboxInfo, UserInfo
from diracx.core.settings import SandboxStoreSettings
from diracx.db.sql.sandbox_metadata.db import SandboxMetadataDB
from diracx.logic.jobs.sandboxes import (
    clean_sandboxes,
    get_sandbox_file,
    initiate_sandbox_upload,
)
from diracx.testing.time import mock_sqlite_time

FAKE_USER_INFO = UserInfo(
    sub="fakevo:97ae90d3-36aa-4271-becf-e61173d93fe3",
    preferred_username="fakeuser",
    dirac_group="fake_group",
    vo="fakevo",
)


@pytest.fixture
async def sandbox_metadata_db() -> AsyncGenerator[SandboxMetadataDB, None]:
    """Create a fake sandbox metadata database."""
    db = SandboxMetadataDB(db_url="sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        sqlalchemy.event.listen(db.engine.sync_engine, "connect", mock_sqlite_time)

        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)

        yield db


@pytest.fixture
async def sandbox_settings(
    test_sandbox_settings,
) -> AsyncGenerator[SandboxStoreSettings, None]:
    """Create a fake sandbox settings."""
    async with test_sandbox_settings.lifetime_function():
        yield test_sandbox_settings


@pytest.fixture()
def frozen_time() -> Generator[freezegun.FreezeGun, None]:
    with freezegun.freeze_time("2012-01-14") as ft:
        yield ft


async def test_upload_and_clean(
    sandbox_metadata_db: SandboxMetadataDB,
    sandbox_settings: SandboxStoreSettings,
    frozen_time: freezegun.FreezeGun,
) -> None:
    """Test the full upload, download, and cleanup workflow.

    This test will create a sandbox, upload it, download it, and then
    clean it up. It will also check that the sandbox is removed from
    the S3 bucket after the cleanup.
    """
    data = secrets.token_bytes(256)
    data_digest = hashlib.sha256(data).hexdigest()
    key = f"fakevo/fake_group/fakeuser/sha256:{data_digest}.tar.zst"
    expected_pfn = f"SB:SandboxSE|/S3/sandboxes/{key}"

    sandbox_info = SandboxInfo(
        checksum_algorithm=ChecksumAlgorithm.SHA256,
        checksum=data_digest,
        size=len(data),
        format=SandboxFormat.TAR_ZST,
    )

    # Test with a new sandbox
    async with sandbox_metadata_db:
        response = await initiate_sandbox_upload(
            FAKE_USER_INFO, sandbox_info, sandbox_metadata_db, sandbox_settings
        )
    assert response.pfn == expected_pfn
    assert response.url is not None

    # Do the actual upload
    files = {"file": ("file", BytesIO(data))}
    async with httpx.AsyncClient() as httpx_client:
        response = await httpx_client.post(
            response.url, data=response.fields, files=files
        )
        response.raise_for_status()

    # Test with the same sandbox again
    async with sandbox_metadata_db:
        response = await initiate_sandbox_upload(
            FAKE_USER_INFO, sandbox_info, sandbox_metadata_db, sandbox_settings
        )
    assert response.pfn == expected_pfn
    assert response.url is None

    # Try to download the sandbox
    async with sandbox_metadata_db:
        download_response = await get_sandbox_file(
            expected_pfn, sandbox_metadata_db, sandbox_settings
        )
    async with httpx.AsyncClient() as httpx_client:
        response = await httpx_client.get(download_response.url)
        response.raise_for_status()
        assert response.content == data

    # There should be no sandboxes to remove
    async with sandbox_metadata_db:
        await clean_sandboxes(sandbox_metadata_db, sandbox_settings)

    # Try to download the sandbox
    async with sandbox_metadata_db:
        download_response = await get_sandbox_file(
            expected_pfn, sandbox_metadata_db, sandbox_settings
        )
    async with httpx.AsyncClient() as httpx_client:
        response = await httpx_client.get(download_response.url)
        response.raise_for_status()
        assert response.content == data

    # Move forward a few weeks
    frozen_time.tick(delta=timedelta(weeks=3))

    # Check that the sandbox exists in the S3 bucket
    await sandbox_settings.s3_client.head_object(
        Bucket=sandbox_settings.bucket_name, Key=key
    )

    # Now the sandbox should be removed
    async with sandbox_metadata_db:
        await clean_sandboxes(sandbox_metadata_db, sandbox_settings)

    # Check that the sandbox was actually removed from the bucket
    with pytest.raises(botocore.exceptions.ClientError, match="Not Found"):
        await sandbox_settings.s3_client.head_object(
            Bucket=sandbox_settings.bucket_name, Key=key
        )

    # Check that the sandbox was removed
    async with sandbox_metadata_db:
        with pytest.raises(SandboxNotFoundError):
            await get_sandbox_file(expected_pfn, sandbox_metadata_db, sandbox_settings)
