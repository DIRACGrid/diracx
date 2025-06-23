from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import bindparam
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import delete, insert, update

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models import (
    PilotFieldsMapping,
    PilotStatus,
    SearchSpec,
    SortSpec,
)

from ..utils import (
    BaseSQLDB,
)
from .schema import (
    JobToPilotMapping,
    PilotAgents,
    PilotAgentsDBBase,
)


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    # ----------------------------- Insert Functions -----------------------------

    async def add_pilots_bulk(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_references: dict[str, str] | None = None,
    ):
        """Bulk add pilots in the DB.

        If we can't find a pilot_reference associated with a stamp, we take the stamp by default.
        """
        if pilot_references is None:
            pilot_references = {}

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {
                "PilotJobReference": pilot_references.get(stamp, stamp),
                "VO": vo,
                "GridType": grid_type,
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": "Submitted",
                "PilotStamp": stamp,
            }
            for stamp in pilot_stamps
        ]

        # Insert multiple rows in a single execute call and use 'returning' to get primary keys
        stmt = insert(PilotAgents).values(values)  # Assuming 'id' is the primary key

        await self.conn.execute(stmt)

    async def add_jobs_to_pilot_bulk(self, job_to_pilot_mapping: list[dict[str, Any]]):
        """Associate a pilot with jobs.

        job_to_pilot_mapping format:
            ```py
            job_to_pilot_mapping = [
                {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            ]
            ```

        Raises:
        - PilotNotFoundError if a pilot_id is not associated with a pilot.
        - PilotAlreadyAssociatedWithJobError if the pilot is already associated with one of the given jobs.
        - NotImplementedError if the integrity error is not caught.

        **Important note**: We assume that a job exists.

        """
        # Insert multiple rows in a single execute call
        stmt = insert(JobToPilotMapping).values(job_to_pilot_mapping)

        try:
            await self.conn.execute(stmt)
        except IntegrityError as e:
            if "foreign key" in str(e.orig).lower():
                raise PilotNotFoundError(
                    data={"pilot_stamps": str(job_to_pilot_mapping)},
                    detail="at least one of these pilots do not exist",
                ) from e

            if (
                "duplicate entry" in str(e.orig).lower()
                or "unique constraint" in str(e.orig).lower()
            ):
                raise PilotAlreadyAssociatedWithJobError(
                    data={"job_to_pilot_mapping": str(job_to_pilot_mapping)}
                ) from e

            # Other errors to catch
            raise NotImplementedError(
                "Engine Specific error not caught" + str(e)
            ) from e

    # ----------------------------- Delete Functions -----------------------------

    async def delete_pilots_by_stamps_bulk(self, pilot_stamps: list[str]):
        """Bulk delete pilots.

        Raises PilotNotFound if one of the pilot was not found.
        """
        stmt = delete(PilotAgents).where(PilotAgents.pilot_stamp.in_(pilot_stamps))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(pilot_stamps):
            raise PilotNotFoundError(data={"pilot_stamps": str(pilot_stamps)})

    async def clear_pilots_bulk(
        self, cutoff_date: datetime, delete_only_aborted: bool = False
    ) -> int:
        """Bulk delete pilots that have SubmissionTime before the 'cutoff_date'.
        Returns the number of deletion.
        """
        # TODO: Add test (Millisec?)
        stmt = delete(PilotAgents).where(PilotAgents.submission_time < cutoff_date)

        # If delete_only_aborted is True, add the condition for 'Status' being 'Aborted'
        if delete_only_aborted:
            stmt = stmt.where(PilotAgents.status == PilotStatus.ABORTED)

        # Execute the statement
        res = await self.conn.execute(stmt)

        return res.rowcount

    # ----------------------------- Update Functions -----------------------------

    async def update_pilot_fields_bulk(
        self, pilot_stamps_to_fields_mapping: list[PilotFieldsMapping]
    ):
        """Bulk update pilots with a mapping.

        pilot_stamps_to_fields_mapping format:
            ```py
            [
                {
                    "PilotStamp": pilot_stamp,
                    "BenchMark": bench_mark,
                    "StatusReason": pilot_reason,
                    "AccountingSent": accounting_sent,
                    "Status": status,
                    "CurrentJobID": current_job_id,
                    "Queue": queue,
                    ...
                }
            ]
            ```

        The mapping helps to update multiple fields at a time.

        Raises PilotNotFoundError if one of the pilots is not found.
        """
        stmt = (
            update(PilotAgents)
            .where(PilotAgents.pilot_stamp == bindparam("b_pilot_stamp"))
            .values(
                {
                    key: bindparam(key)
                    for key in pilot_stamps_to_fields_mapping[0]
                    .model_dump(exclude_none=True)
                    .keys()
                    if key != "PilotStamp"
                }
            )
        )

        values = [
            {
                **{"b_pilot_stamp": mapping.PilotStamp},
                **mapping.model_dump(exclude={"PilotStamp"}, exclude_none=True),
            }
            for mapping in pilot_stamps_to_fields_mapping
        ]

        res = await self.conn.execute(stmt, values)

        if res.rowcount != len(pilot_stamps_to_fields_mapping):
            raise PilotNotFoundError(
                data={"mapping": str(pilot_stamps_to_fields_mapping)}
            )

    # ----------------------------- Search Functions -----------------------------

    async def search_pilots(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        """Search for pilots in the database."""
        return await self.search(
            model=PilotAgents,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )

    async def search_pilot_to_job_mapping(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        """Search for pilots in the database."""
        return await self.search(
            model=JobToPilotMapping,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )
