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
    PilotOutput,
)


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    # ----------------------------- Insert Functions -----------------------------

    async def add_pilots(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        grid_site: str = "Unknown",
        destination_site: str = "NotAssigned",
        pilot_references: dict[str, str] | None = None,
        status: str = PilotStatus.SUBMITTED,
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
                "GridSite": grid_site,
                "DestinationSite": destination_site,
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": status,
                "PilotStamp": stamp,
            }
            for stamp in pilot_stamps
        ]

        # Insert multiple rows in a single execute call and use 'returning' to get primary keys
        stmt = insert(PilotAgents).values(values)  # Assuming 'id' is the primary key

        await self.conn.execute(stmt)

    async def add_jobs_to_pilot(self, job_to_pilot_mapping: list[dict[str, Any]]):
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

    async def delete_pilots(self, pilot_ids: list[int]):
        """Destructive function. Delete pilots."""
        stmt = delete(PilotAgents).where(PilotAgents.pilot_id.in_(pilot_ids))

        await self.conn.execute(stmt)

    async def remove_jobs_from_pilots(self, pilot_ids: list[int]):
        """Destructive function. De-associate jobs and pilots."""
        stmt = delete(JobToPilotMapping).where(
            JobToPilotMapping.pilot_id.in_(pilot_ids)
        )

        await self.conn.execute(stmt)

    async def delete_pilot_logs(self, pilot_ids: list[int]):
        """Destructive function. Remove logs from pilots."""
        stmt = delete(PilotOutput).where(PilotOutput.pilot_id.in_(pilot_ids))

        await self.conn.execute(stmt)

    # ----------------------------- Update Functions -----------------------------

    async def update_pilot_fields(
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
        """Search for pilot information in the database."""
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
        """Search for jobs that are associated with pilots."""
        return await self.search(
            model=JobToPilotMapping,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )

    async def pilot_summary(
        self, group_by: list[str], search: list[SearchSpec]
    ) -> list[dict[str, str | int]]:
        """Get a summary of the pilots."""
        return await self.summary(PilotAgents, group_by=group_by, search=search)
