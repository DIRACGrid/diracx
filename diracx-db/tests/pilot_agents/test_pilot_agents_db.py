from __future__ import annotations

import pytest

from diracx.db.sql.pilot_agents.db import PilotAgentsDB


@pytest.fixture
async def pilot_agents_db(tmp_path) -> PilotAgentsDB:
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


async def test_insert_and_select(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        # Add a pilot reference
        refs = [f"ref_{i}" for i in range(10)]
        stamps = [f"stamp_{i}" for i in range(10)]
        stamp_dict = dict(zip(refs, stamps))

        await pilot_agents_db.add_pilot_references(
            refs, "test_vo", grid_type="DIRAC", pilot_stamps=stamp_dict
        )

        await pilot_agents_db.add_pilot_references(
            refs, "test_vo", grid_type="DIRAC", pilot_stamps=None
        )


async def test_jobs_association(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        # Create two pilots
        await pilot_agents_db.add_pilot_references(["ref"], "lhcb", grid_type="DIRAC")

        await pilot_agents_db.add_pilot_references(["ref2"], "lhcb", grid_type="DIRAC")

        # Get both pilot infos
        pilot = await pilot_agents_db.get_pilot_by_reference("ref")
        pilot_id = pilot["PilotID"]
        # --------
        pilot_2 = await pilot_agents_db.get_pilot_by_reference("ref2")
        pilot_id_2 = pilot_2["PilotID"]

        # We do not need *for now* to insert jobs: no foreign keys or verifications
        job_ids = set([i for i in range(10)])
        job_ids_2 = set([i for i in range(100, 110)])

        # Associate pilots with jobs
        await pilot_agents_db.associate_pilot_with_jobs(
            pilot_id=pilot_id, job_ids=job_ids
        )

        # We associate another pilot, to make sure that we only fetch the wanted pilot
        await pilot_agents_db.associate_pilot_with_jobs(
            pilot_id=pilot_id_2, job_ids=job_ids_2
        )

        # Get the jobs associated with the first pilot
        fetched_job_ids = await pilot_agents_db.get_pilot_job_ids(pilot_id)
        # Convert to set, it is easier to play with
        fetched_job_ids_set = set(fetched_job_ids)
        job_ids_set = set(job_ids)

        # Asserts
        assert len(fetched_job_ids) == len(job_ids)
        assert fetched_job_ids_set == job_ids_set

        # Get the jobs associated with the second pilot
        fetched_job_ids_2 = await pilot_agents_db.get_pilot_job_ids(pilot_id_2)
        # Convert to set, it is easier to play with
        fetched_job_ids_set_2 = set(fetched_job_ids_2)
        job_ids_set_2 = set(job_ids_2)

        # Asserts
        assert len(fetched_job_ids) == len(job_ids_2)
        assert fetched_job_ids_set_2 == job_ids_set_2
