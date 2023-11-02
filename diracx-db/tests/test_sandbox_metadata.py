from __future__ import annotations

import asyncio
import secrets
from datetime import datetime

import pytest
import sqlalchemy

from diracx.core.models import SandboxInfo, UserInfo
from diracx.db.sql.sandbox_metadata.db import SandboxMetadataDB
from diracx.db.sql.sandbox_metadata.schema import sb_SandBoxes


@pytest.fixture
async def sandbox_metadata_db(tmp_path):
    sandbox_metadata_db = SandboxMetadataDB("sqlite+aiosqlite:///:memory:")
    async with sandbox_metadata_db.engine_context():
        async with sandbox_metadata_db.engine.begin() as conn:
            await conn.run_sync(sandbox_metadata_db.metadata.create_all)
        yield sandbox_metadata_db


def test_get_pfn(sandbox_metadata_db: SandboxMetadataDB):
    user_info = UserInfo(
        sub="vo:sub", preferred_username="user1", dirac_group="group1", vo="vo"
    )
    sandbox_info = SandboxInfo(
        checksum="checksum",
        checksum_algorithm="sha256",
        format="tar.bz2",
        size=100,
    )
    pfn = sandbox_metadata_db.get_pfn("bucket1", user_info, sandbox_info)
    assert pfn == "/S3/bucket1/vo/group1/user1/sha256:checksum.tar.bz2"


async def test_insert_sandbox(sandbox_metadata_db: SandboxMetadataDB):
    user_info = UserInfo(
        sub="vo:sub", preferred_username="user1", dirac_group="group1", vo="vo"
    )
    pfn1 = secrets.token_hex()

    # Make sure the sandbox doesn't already exist
    db_contents = await _dump_db(sandbox_metadata_db)
    assert pfn1 not in db_contents
    async with sandbox_metadata_db:
        with pytest.raises(sqlalchemy.exc.NoResultFound):
            await sandbox_metadata_db.sandbox_is_assigned("SandboxSE", pfn1)

    # Insert the sandbox
    async with sandbox_metadata_db:
        await sandbox_metadata_db.insert_sandbox("SandboxSE", user_info, pfn1, 100)
    db_contents = await _dump_db(sandbox_metadata_db)
    owner_id1, last_access_time1 = db_contents[pfn1]

    # Inserting again should update the last access time
    await asyncio.sleep(1)  # The timestamp only has second precision
    async with sandbox_metadata_db:
        await sandbox_metadata_db.insert_sandbox("SandboxSE", user_info, pfn1, 100)
    db_contents = await _dump_db(sandbox_metadata_db)
    owner_id2, last_access_time2 = db_contents[pfn1]
    assert owner_id1 == owner_id2
    assert last_access_time2 > last_access_time1

    # The sandbox still hasn't been assigned
    async with sandbox_metadata_db:
        assert not await sandbox_metadata_db.sandbox_is_assigned("SandboxSE", pfn1)

    # Inserting again should update the last access time
    await asyncio.sleep(1)  # The timestamp only has second precision
    last_access_time3 = (await _dump_db(sandbox_metadata_db))[pfn1][1]
    assert last_access_time2 == last_access_time3
    async with sandbox_metadata_db:
        await sandbox_metadata_db.update_sandbox_last_access_time("SandboxSE", pfn1)
    last_access_time4 = (await _dump_db(sandbox_metadata_db))[pfn1][1]
    assert last_access_time2 < last_access_time4


async def _dump_db(
    sandbox_metadata_db: SandboxMetadataDB,
) -> dict[str, tuple[int, datetime]]:
    """Dump the contents of the sandbox metadata database

    Returns a dict[pfn: str, (owner_id: int, last_access_time: datetime)]
    """
    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(
            sb_SandBoxes.SEPFN, sb_SandBoxes.OwnerId, sb_SandBoxes.LastAccessTime
        )
        res = await sandbox_metadata_db.conn.execute(stmt)
        return {row.SEPFN: (row.OwnerId, row.LastAccessTime) for row in res}
