"""SQL database operations for pilot registration and job associations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import case, insert, literal, select, update
from sqlalchemy.exc import IntegrityError

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models.pilot import PilotStatus
from diracx.core.models.search import SearchSpec, SortSpec

from ..utils import BaseSQLDB
from .schema import (
    JobToPilotMapping,
    PilotAgents,
    PilotAgentsDBBase,
)


class PilotAgentsDB(BaseSQLDB):
    """Database operations for pilot records and pilot-job mappings.

    Attributes:
        metadata: SQLAlchemy metadata containing the pilot database tables.
    """

    metadata = PilotAgentsDBBase.metadata

    async def register_pilots(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        grid_site: str = "Unknown",
        destination_site: str = "NotAssigned",
        pilot_references: dict[str, str] | None = None,
        status: PilotStatus = PilotStatus.SUBMITTED,
    ):
        """Bulk-register pilots.

        If a stamp has no entry in `pilot_references` the stamp is used as
        the reference.

        Args:
            pilot_stamps: Stamps identifying the pilots to register.
            vo: Virtual organization associated with the pilots.
            grid_type: Grid type for the pilots.
            grid_site: Grid site for the pilots.
            destination_site: Destination site assigned to the pilots.
            pilot_references: Optional mapping from pilot stamps to references.
            status: Initial status for the registered pilots.
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
        Raises PilotAlreadyAssociatedWithJobError on duplicates. The legacy
        schema has no foreign key on JobToPilotMapping, so the caller must
        ensure the pilots and jobs exist.

        Args:
            job_to_pilot_mapping: Pilot-job association records to insert.

        Raises:
            PilotAlreadyAssociatedWithJobError: If an association already exists.
        """
        stmt = insert(JobToPilotMapping).values(job_to_pilot_mapping)

        try:
            await self.conn.execute(stmt)
        except IntegrityError as e:
            msg = str(e.orig).lower()
            if "duplicate entry" in msg or "unique constraint" in msg:
                raise PilotAlreadyAssociatedWithJobError(
                    detail=(
                        "at least one of these pilots is already associated "
                        "with a given job."
                    )
                ) from e
            raise

    async def update_pilot_metadata(self, updates: dict[str, dict[str, Any]]):
        """Bulk-update pilot fields.

        `updates` maps a pilot stamp to the column/value pairs to set for
        that pilot; each entry may set a different subset of columns. Uses
        a per-column CASE expression to support heterogeneous updates,
        matching the pattern in JobDB.set_job_attributes. Raises
        PilotNotFoundError if any of the pilot stamps is not found.

        Args:
            updates: Mapping from pilot stamps to column/value updates.

        Raises:
            PilotNotFoundError: If any pilot stamp is not found.
        """
        if not updates:
            return

        columns = {col for fields in updates.values() for col in fields}

        case_expressions = {
            column: case(
                *[
                    (
                        PilotAgents.__table__.c.PilotStamp == stamp,
                        literal(
                            fields[column],
                            type_=PilotAgents.__table__.c[column].type,
                        ),
                    )
                    for stamp, fields in updates.items()
                    if column in fields
                ],
                else_=getattr(PilotAgents.__table__.c, column),
            )
            for column in columns
        }

        stmt = (
            update(PilotAgents)
            .values(**case_expressions)
            .where(PilotAgents.__table__.c.PilotStamp.in_(updates.keys()))
        )
        result = await self.conn.execute(stmt)

        if result.rowcount != len(updates):
            raise PilotNotFoundError("at least one of the given pilots does not exist.")

    async def search(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Search for pilot information in the database.

        Args:
            parameters: Optional pilot fields to include in each result.
            search: Search conditions to apply.
            sorts: Sort specifications to apply.
            distinct: Whether to return distinct results.
            per_page: Maximum number of results per page.
            page: Optional one-based page number for pagination.

        Returns:
            Total matching pilot count and the requested page of records.
        """
        return await self._search(
            table=PilotAgents,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )

    async def summary(
        self, group_by: list[str], search: list[SearchSpec]
    ) -> list[dict[str, str | int]]:
        """Aggregate pilot counts by the requested columns.

        Args:
            group_by: Pilot fields used to group the summary.
            search: Search conditions to apply before aggregation.

        Returns:
            Summary rows containing grouped values and counts.
        """
        return await self._summary(table=PilotAgents, group_by=group_by, search=search)

    async def get_job_ids_for_stamps(self, pilot_stamps: list[str]) -> list[int]:
        """Return the IDs of jobs that have run on any of the given pilot stamps.

        Single round-trip SQL join over JobToPilotMapping and PilotAgents
        (both live in the same metadata, so the join is legitimate at the
        DB layer).

        Args:
            pilot_stamps: Pilot stamps to find associated jobs for.

        Returns:
            Distinct job identifiers associated with the supplied pilots.
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

    async def get_pilot_ids_for_job_ids(self, job_ids: list[int]) -> list[int]:
        """Return the IDs of pilots that have run any of the given jobs.

        Args:
            job_ids: Job identifiers to find associated pilots for.

        Returns:
            Distinct pilot identifiers associated with the supplied jobs.
        """
        if not job_ids:
            return []
        stmt = (
            select(JobToPilotMapping.pilot_id)
            .where(JobToPilotMapping.job_id.in_(job_ids))
            .distinct()
        )
        result = await self.conn.execute(stmt)
        return [row[0] for row in result]
