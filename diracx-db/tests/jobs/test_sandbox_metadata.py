from __future__ import annotations

import asyncio
import secrets
from datetime import datetime

import pytest
import sqlalchemy

from diracx.core.exceptions import SandboxAlreadyInsertedError, SandboxNotFoundError
from diracx.core.models import SandboxInfo, UserInfo
from diracx.db.sql.sandbox_metadata.db import SandboxMetadataDB
from diracx.db.sql.sandbox_metadata.schema import SandBoxes, SBEntityMapping


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
        checksum="90e0ba6763c91a905bb9fd6e025aac1952ae742e6d756a31a0963aa7df7cd7b1",
        checksum_algorithm="sha256",
        format="tar.bz2",
        size=100,
    )
    pfn = sandbox_metadata_db.get_pfn("bucket1", user_info, sandbox_info)
    assert pfn == (
        "/S3/bucket1/vo/group1/user1/"
        "sha256:90e0ba6763c91a905bb9fd6e025aac1952ae742e6d756a31a0963aa7df7cd7b1.tar.bz2"
    )


async def test_insert_sandbox(sandbox_metadata_db: SandboxMetadataDB):
    # TODO: DAL tests should be very simple, such complex tests should be handled in diracx-routers
    user_info = UserInfo(
        sub="vo:sub", preferred_username="user1", dirac_group="group1", vo="vo"
    )
    pfn1 = secrets.token_hex()

    # Make sure the sandbox doesn't already exist
    db_contents = await _dump_db(sandbox_metadata_db)
    assert pfn1 not in db_contents
    async with sandbox_metadata_db:
        with pytest.raises(SandboxNotFoundError):
            await sandbox_metadata_db.sandbox_is_assigned(pfn1, "SandboxSE")

    # Insert owner
    async with sandbox_metadata_db:
        owner_id = await sandbox_metadata_db.insert_owner(user_info)
    assert owner_id == 1

    # Insert the sandbox
    async with sandbox_metadata_db:
        await sandbox_metadata_db.insert_sandbox(owner_id, "SandboxSE", pfn1, 100)
    db_contents = await _dump_db(sandbox_metadata_db)
    owner_id1, last_access_time1 = db_contents[pfn1]

    await asyncio.sleep(1)  # The timestamp only has second precision
    async with sandbox_metadata_db:
        with pytest.raises(SandboxAlreadyInsertedError):
            await sandbox_metadata_db.insert_sandbox(owner_id, "SandboxSE", pfn1, 100)

        await sandbox_metadata_db.update_sandbox_last_access_time("SandboxSE", pfn1)

    db_contents = await _dump_db(sandbox_metadata_db)
    owner_id2, last_access_time2 = db_contents[pfn1]
    assert owner_id1 == owner_id2
    assert last_access_time2 > last_access_time1

    # The sandbox still hasn't been assigned
    async with sandbox_metadata_db:
        assert not await sandbox_metadata_db.sandbox_is_assigned(pfn1, "SandboxSE")

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
    """Dump the contents of the sandbox metadata database.

    Returns a dict[pfn: str, (owner_id: int, last_access_time: datetime)]
    """
    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(
            SandBoxes.SEPFN, SandBoxes.OwnerId, SandBoxes.LastAccessTime
        )
        res = await sandbox_metadata_db.conn.execute(stmt)
        return {row.SEPFN: (row.OwnerId, row.LastAccessTime) for row in res}


async def test_assign_and_unsassign_sandbox_to_jobs(
    sandbox_metadata_db: SandboxMetadataDB,
):
    # TODO: DAL tests should be very simple, such complex tests should be handled in diracx-routers
    pfn = secrets.token_hex()
    user_info = UserInfo(
        sub="vo:sub", preferred_username="user1", dirac_group="group1", vo="vo"
    )
    dummy_jobid = 666
    sandbox_se = "SandboxSE"
    # Insert the sandbox
    async with sandbox_metadata_db:
        owner_id = await sandbox_metadata_db.insert_owner(user_info)
        await sandbox_metadata_db.insert_sandbox(owner_id, sandbox_se, pfn, 100)

    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(SandBoxes.SBId, SandBoxes.SEPFN)
        res = await sandbox_metadata_db.conn.execute(stmt)
    db_contents = {row.SEPFN: row.SBId for row in res}
    sb_id_1 = db_contents[pfn]
    # The sandbox still hasn't been assigned
    async with sandbox_metadata_db:
        assert not await sandbox_metadata_db.sandbox_is_assigned(pfn, sandbox_se)

    # Check there is no mapping
    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(
            SBEntityMapping.SBId, SBEntityMapping.EntityId, SBEntityMapping.Type
        )
        res = await sandbox_metadata_db.conn.execute(stmt)
    db_contents = {row.SBId: (row.EntityId, row.Type) for row in res}
    assert db_contents == {}

    # Assign sandbox with dummy jobid
    async with sandbox_metadata_db:
        await sandbox_metadata_db.assign_sandbox_to_jobs(
            jobs_ids=[dummy_jobid], pfn=pfn, sb_type="Output", se_name=sandbox_se
        )
    # Check if sandbox and job are mapped
    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(
            SBEntityMapping.SBId, SBEntityMapping.EntityId, SBEntityMapping.Type
        )
        res = await sandbox_metadata_db.conn.execute(stmt)
    db_contents = {row.SBId: (row.EntityId, row.Type) for row in res}

    entity_id_1, sb_type = db_contents[sb_id_1]
    assert entity_id_1 == f"Job:{dummy_jobid}"
    assert sb_type == "Output"

    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(SandBoxes.SBId, SandBoxes.SEPFN)
        res = await sandbox_metadata_db.conn.execute(stmt)
    db_contents = {row.SEPFN: row.SBId for row in res}
    sb_id_1 = db_contents[pfn]
    # The sandbox should be assigned
    async with sandbox_metadata_db:
        assert await sandbox_metadata_db.sandbox_is_assigned(pfn, sandbox_se)

    # Unassign the sandbox to job
    async with sandbox_metadata_db:
        await sandbox_metadata_db.unassign_sandboxes_to_jobs([dummy_jobid])

    # Entity should not exists anymore
    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(SBEntityMapping.SBId).where(
            SBEntityMapping.EntityId == entity_id_1
        )
        res = await sandbox_metadata_db.conn.execute(stmt)
    entity_sb_id = [row.SBId for row in res]
    assert entity_sb_id == []

    # Should not be assigned anymore
    async with sandbox_metadata_db:
        assert await sandbox_metadata_db.sandbox_is_assigned(pfn, sandbox_se) is False
    # Check the mapping has been deleted
    async with sandbox_metadata_db:
        stmt = sqlalchemy.select(SBEntityMapping.SBId)
        res = await sandbox_metadata_db.conn.execute(stmt)
    res_sb_id = [row.SBId for row in res]
    assert sb_id_1 not in res_sb_id


async def test_get_sandbox_owner_id(sandbox_metadata_db: SandboxMetadataDB):
    user_info = UserInfo(
        sub="vo:sub", preferred_username="user1", dirac_group="group1", vo="vo"
    )
    pfn = secrets.token_hex()
    sandbox_se = "SandboxSE"
    # Insert the sandbox
    async with sandbox_metadata_db:
        owner_id = await sandbox_metadata_db.insert_owner(user_info)
        await sandbox_metadata_db.insert_sandbox(owner_id, sandbox_se, pfn, 100)

    async with sandbox_metadata_db:
        sb_owner_id = await sandbox_metadata_db.get_sandbox_owner_id(pfn, sandbox_se)

    assert owner_id == 1
    assert sb_owner_id == owner_id

    async with sandbox_metadata_db:
        sb_owner_id = await sandbox_metadata_db.get_sandbox_owner_id(
            "not_found", sandbox_se
        )
    assert sb_owner_id is None
