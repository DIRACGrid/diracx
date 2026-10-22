from __future__ import annotations

import pytest
from sqlalchemy import func, select, update

from diracx.core.cwl import decompose, format_reference, unpack
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import Processes
from diracx.logic.transformation.processes import (
    ProcessNotFoundError,
    load_process,
    store_process,
)


@pytest.fixture
async def transformation_db():
    db = TransformationDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        yield db


def tool(command: str) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": [command],
        "inputs": {"files": "File[]"},
        "outputs": {"out": {"type": "File", "outputBinding": {"glob": "*"}}},
    }


def transformation(*runs) -> dict:
    return {
        "cwlVersion": "v1.2",
        "class": "Workflow",
        "inputs": {"files": "File[]"},
        "outputs": {"out": {"type": "File", "outputSource": "s0/out"}},
        "steps": {
            f"s{i}": {"run": run, "in": {"files": "files"}, "out": ["out"]}
            for i, run in enumerate(runs)
        },
    }


async def count_processes(db: TransformationDB) -> int:
    return await db.conn.scalar(select(func.count()).select_from(Processes))


async def test_store_then_load_round_trips(transformation_db: TransformationDB):
    process = transformation(tool("gauss"), tool("boole"))
    async with transformation_db as db:
        process_id = await store_process(db, process)
        packed = await load_process(db, process_id)
    assert process_id == decompose(process)[0]
    assert unpack(packed) == decompose(process)


async def test_one_row_per_transformation_deduplicated(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        first = await store_process(db, transformation(tool("hadd")))
        again = await store_process(db, transformation(tool("hadd")))
        other = await store_process(db, transformation(tool("hadd"), tool("hadd")))
        assert first == again != other
        assert await count_processes(db) == 2


async def test_reference_to_a_stored_process_is_inlined(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        library = await store_process(db, tool("hadd"))
        by_reference = await store_process(
            db, transformation(format_reference(library))
        )
        inline = await load_process(db, by_reference)
    assert by_reference == decompose(transformation(tool("hadd")))[0]
    assert len(inline["$graph"]) == 2


async def test_reference_to_an_unknown_process_is_refused(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        with pytest.raises(ProcessNotFoundError):
            await store_process(db, transformation(format_reference(b"x" * 32)))


async def test_loading_an_unknown_process_is_refused(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        with pytest.raises(ProcessNotFoundError):
            await load_process(db, b"x" * 32)


async def test_unresolved_directives_are_refused_before_anything_is_read(
    transformation_db: TransformationDB,
):
    process = tool("cat")
    process["requirements"] = {
        "InitialWorkDirRequirement": {
            "listing": [{"entryname": "f", "entry": {"$include": "/etc/hosts"}}]
        }
    }
    async with transformation_db as db:
        with pytest.raises(ValueError, match=r"Unresolved \$include"):
            await store_process(db, process)
        assert await count_processes(db) == 0


async def test_a_stored_row_that_does_not_match_its_identity_is_refused(
    transformation_db: TransformationDB,
):
    async with transformation_db as db:
        stored = await store_process(db, tool("hadd"))
        other = await store_process(db, tool("cat"))
        swapped = (await db.get_processes([other]))[other]
        await db.conn.execute(
            update(Processes)
            .where(Processes.process_id == stored)
            .values({Processes.process: swapped})
        )
        with pytest.raises(ValueError, match="does not match its content"):
            await store_process(db, transformation(format_reference(stored)))
