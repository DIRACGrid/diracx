from __future__ import annotations

__all__ = ["PilotAgentsDB"]

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import case, delete, insert, literal, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import expression

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models.pilot import PilotMetadata, PilotStatus
from diracx.core.models.search import SearchSpec, SortSpec

from ..utils import BaseSQLDB
from .schema import (
    JobToPilotMapping,
    PilotAgents,
    PilotAgentsDBBase,
    PilotOutput,
)


class PilotAgentsDB(BaseSQLDB):
    """Front-end to the PilotAgents database."""

    metadata = PilotAgentsDBBase.metadata

    async def register_pilots(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        grid_site: str = "Unknown",
        destination_site: str = "NotAssigned",
        pilot_references: dict[str, str] | None = None,
        status: str = PilotStatus.SUBMITTED,
    ):
        """Bulk-register pilots.

        If a stamp has no entry in `pilot_references` the stamp is used as
        the reference.
        """
        if pilot_references is None:
            pilot_references = {}

        now = datetime.now(tz=timezone.utc)

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

        await self.conn.execute(insert(PilotAgents).values(values))

    async def assign_jobs_to_pilot(self, job_to_pilot_mapping: list[dict[str, Any]]):
        """Associate a pilot with jobs.

        Each entry has the shape `{"PilotID": ..., "JobID": ..., "StartTime": ...}`.
        Raises PilotNotFoundError if any pilot is missing, and
        PilotAlreadyAssociatedWithJobError on duplicates. Caller must
        ensure the jobs exist.
        """
        stmt = insert(JobToPilotMapping).values(job_to_pilot_mapping)

        try:
            await self.conn.execute(stmt)
        except IntegrityError as e:
            msg = str(e.orig).lower()
            if "foreign key" in msg:
                raise PilotNotFoundError(
                    detail="at least one of these pilots does not exist",
                ) from e
            if "duplicate entry" in msg or "unique constraint" in msg:
                raise PilotAlreadyAssociatedWithJobError(
                    detail=(
                        "at least one of these pilots is already associated "
                        "with a given job."
                    )
                ) from e
            raise

    async def delete_pilots(self, pilot_ids: list[int]):
        """Destructive. Delete pilots by ID."""
        await self.conn.execute(
            delete(PilotAgents).where(PilotAgents.pilot_id.in_(pilot_ids))
        )

    async def remove_jobs_from_pilots(self, pilot_ids: list[int]):
        """Destructive. De-associate jobs and pilots."""
        await self.conn.execute(
            delete(JobToPilotMapping).where(JobToPilotMapping.pilot_id.in_(pilot_ids))
        )

    async def delete_pilot_logs(self, pilot_ids: list[int]):
        """Destructive. Remove pilot logs."""
        await self.conn.execute(
            delete(PilotOutput).where(PilotOutput.pilot_id.in_(pilot_ids))
        )

    async def update_pilot_metadata(self, pilot_metadata: list[PilotMetadata]):
        """Bulk-update pilot metadata.

        Each PilotMetadata entry may set a different subset of fields;
        unset fields (None) are preserved. Uses a per-column CASE
        expression to support heterogeneous updates, matching the pattern
        in JobDB.set_job_attributes. Raises PilotNotFoundError if any of
        the pilot stamps is not found.
        """
        if not pilot_metadata:
            return

        updates_by_stamp: dict[str, dict[str, Any]] = {
            m.PilotStamp: m.model_dump(exclude={"PilotStamp"}, exclude_none=True)
            for m in pilot_metadata
        }

        columns = {col for fields in updates_by_stamp.values() for col in fields}
        if not columns:
            return

        case_expressions = {
            column: case(
                *[
                    (
                        PilotAgents.__table__.c.PilotStamp == stamp,
                        literal(
                            fields[column],
                            type_=PilotAgents.__table__.c[column].type,
                        )
                        if not isinstance(fields[column], expression.FunctionElement)
                        else fields[column],
                    )
                    for stamp, fields in updates_by_stamp.items()
                    if column in fields
                ],
                else_=getattr(PilotAgents.__table__.c, column),
            )
            for column in columns
        }

        stmt = (
            update(PilotAgents)
            .values(**case_expressions)
            .where(PilotAgents.__table__.c.PilotStamp.in_(updates_by_stamp.keys()))
        )
        result = await self.conn.execute(stmt)

        if result.rowcount != len(updates_by_stamp):
            raise PilotNotFoundError("at least one of the given pilots does not exist.")

    async def search_pilots(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Search for pilot information in the database."""
        return await self._search(
            table=PilotAgents,
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
        """Aggregate pilot counts by the requested columns."""
        return await self._summary(table=PilotAgents, group_by=group_by, search=search)

    async def job_ids_for_stamps(self, pilot_stamps: list[str]) -> list[int]:
        """Return the IDs of jobs that have run on any of the given pilot stamps.

        Single round-trip SQL join over JobToPilotMapping and PilotAgents
        (both live in the same metadata, so the join is legitimate at the
        DB layer).
        """
        if not pilot_stamps:
            return []
        stmt = (
            select(JobToPilotMapping.job_id)
            .join(
                PilotAgents,
                PilotAgents.pilot_id == JobToPilotMapping.pilot_id,
            )
            .where(PilotAgents.pilot_stamp.in_(pilot_stamps))
            .distinct()
        )
        result = await self.conn.execute(stmt)
        return [row[0] for row in result]

    async def pilot_ids_for_job_ids(self, job_ids: list[int]) -> list[int]:
        """Return the IDs of pilots that have run any of the given jobs."""
        if not job_ids:
            return []
        stmt = (
            select(JobToPilotMapping.pilot_id)
            .where(JobToPilotMapping.job_id.in_(job_ids))
            .distinct()
        )
        result = await self.conn.execute(stmt)
        return [row[0] for row in result]
