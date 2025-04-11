from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import insert, select

from ..utils import BaseSQLDB
from .schema import JobToPilotMapping, PilotAgents, PilotAgentsDBBase


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    async def add_pilot_references(
        self,
        pilot_ref: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_stamps: dict | None = None,
    ) -> None:

        if pilot_stamps is None:
            pilot_stamps = {}

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {
                "PilotJobReference": ref,
                "VO": vo,
                "GridType": grid_type,
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": "Submitted",
                "PilotStamp": pilot_stamps.get(ref, ""),
            }
            for ref in pilot_ref
        ]

        # Insert multiple rows in a single execute call
        stmt = insert(PilotAgents).values(values)
        await self.conn.execute(stmt)

    async def get_pilot_by_reference(self, pilot_ref: str) -> dict:
        stmt = select(PilotAgents).where(PilotAgents.pilot_job_reference == pilot_ref)

        # Execute the query and fetch one result
        result = await self.conn.execute(stmt)

        # Get the first row, which should be the pilot if it exists
        pilot = result.one()

        return dict(pilot._mapping)

    async def get_pilot_job_ids(self, pilot_id: set) -> list[int]:
        stmt = select(JobToPilotMapping.job_id).where(
            JobToPilotMapping.pilot_id == pilot_id
        )

        # Execute the results
        result = await self.conn.execute(stmt)

        return list(result.scalars().all())

    async def associate_pilot_with_jobs(self, pilot_id: int, job_ids: list[int]):

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            for job_id in job_ids
        ]

        # Insert multiple rows in a single execute call
        stmt = insert(JobToPilotMapping).values(values)
        await self.conn.execute(stmt)
