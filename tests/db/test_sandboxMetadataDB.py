from __future__ import annotations

import pytest
import sqlalchemy

from diracx.db.sql.sandbox_metadata.db import SandboxMetadataDB


@pytest.fixture
async def sandbox_metadata_db(tmp_path):
    sandbox_metadata_db = SandboxMetadataDB("sqlite+aiosqlite:///:memory:")
    async with sandbox_metadata_db.engine_context():
        async with sandbox_metadata_db.engine.begin() as conn:
            await conn.run_sync(sandbox_metadata_db.metadata.create_all)
        yield sandbox_metadata_db


async def test__get_put_owner(sandbox_metadata_db):
    async with sandbox_metadata_db as sandbox_metadata_db:
        result = await sandbox_metadata_db._get_put_owner("owner", "owner_group")
        assert result == 1
        result = await sandbox_metadata_db._get_put_owner("owner_2", "owner_group")
        assert result == 2
        result = await sandbox_metadata_db._get_put_owner("owner", "owner_group")
        assert result == 1
        result = await sandbox_metadata_db._get_put_owner("owner_2", "owner_group")
        assert result == 2
        result = await sandbox_metadata_db._get_put_owner("owner_2", "owner_group_2")
        assert result == 3


async def test_insert(sandbox_metadata_db):
    async with sandbox_metadata_db as sandbox_metadata_db:
        result = await sandbox_metadata_db.insert(
            "owner",
            "owner_group",
            "sbSE",
            "sbPFN",
            123,
        )
        assert result == 1

        result = await sandbox_metadata_db.insert(
            "owner",
            "owner_group",
            "sbSE",
            "sbPFN",
            123,
        )
        assert result == 1

        result = await sandbox_metadata_db.insert(
            "owner_2",
            "owner_group",
            "sbSE",
            "sbPFN_2",
            123,
        )
        assert result == 2

        # This would be incorrect
        with pytest.raises(sqlalchemy.exc.NoResultFound):
            await sandbox_metadata_db.insert(
                "owner",
                "owner_group",
                "sbSE",
                "sbPFN_2",
                123,
            )


async def test_delete(sandbox_metadata_db):
    async with sandbox_metadata_db as sandbox_metadata_db:
        result = await sandbox_metadata_db.insert(
            "owner",
            "owner_group",
            "sbSE",
            "sbPFN",
            123,
        )
        assert result == 1

        result = await sandbox_metadata_db.delete([1])
        assert result
