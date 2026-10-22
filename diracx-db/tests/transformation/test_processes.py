from __future__ import annotations

from sqlalchemy import func, select

from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import Processes


async def test_insert_processes_is_idempotent(transformation_db: TransformationDB):
    a, b = b"a" * 32, b"b" * 32
    async with transformation_db as db:
        await db.insert_processes({a: {"class": "Workflow"}})
        await db.insert_processes({a: {"class": "Workflow"}, b: {"class": "Tool"}})
        await db.insert_processes({})
        count = await db.conn.scalar(select(func.count()).select_from(Processes))
    assert count == 2


async def test_get_processes_returns_only_what_is_stored(
    transformation_db: TransformationDB,
):
    a = b"a" * 32
    async with transformation_db as db:
        await db.insert_processes({a: {"class": "Workflow", "steps": {}}})
        found = await db.get_processes([a, b"z" * 32])
    assert found == {a: {"class": "Workflow", "steps": {}}}
