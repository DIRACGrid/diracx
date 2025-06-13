from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import RowMapping, bindparam, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import delete, insert, select, update

from diracx.core.exceptions import (
    InvalidQueryError,
    PilotAlreadyAssociatedWithJobError,
    PilotJobsNotFoundError,
    PilotNotFoundError,
)
from diracx.core.models import (
    PilotFieldsMapping,
    SearchSpec,
    SortSpec,
)

from ..utils import (
    BaseSQLDB,
    _get_columns,
    apply_search_filters,
    apply_sort_constraints,
    fetch_records_bulk_or_raises,
)
from .schema import (
    JobToPilotMapping,
    PilotAgents,
    PilotAgentsDBBase,
)


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    async def add_pilots_bulk(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_references: dict | None = None,
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

    async def delete_pilots_by_stamps_bulk(self, pilot_stamps: list[str]):
        """Bulk delete pilots.

        Raises PilotNotFound if one of the pilot was not found.
        """
        stmt = delete(PilotAgents).where(PilotAgents.pilot_stamp.in_(pilot_stamps))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(pilot_stamps):
            raise PilotNotFoundError(data={"pilot_stamps": str(pilot_stamps)})

    async def associate_pilot_with_jobs(self, job_to_pilot_mapping: list[dict]):
        """Associate a pilot with jobs.

        job_to_pilot_mapping format:
            ```py
            job_to_pilot_mapping = [
                {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            ]
            ```

        Raises:
        - PilotNotFoundError if a pilot_id is not associated with a pilot.
        - PilotAlreadyAssociatedWithJobError if the pilot is already associated with a job.
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
                    detail="at least one of these pilots does not exist",
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

    async def get_pilots_by_stamp_bulk(
        self, pilot_stamps: list[str]
    ) -> Sequence[RowMapping]:
        """Bulk fetch pilots.

        Raises PilotNotFoundError if one of the stamp is not associated with a pilot.

        """
        results = await fetch_records_bulk_or_raises(
            self.conn,
            PilotAgents,
            PilotNotFoundError,
            "pilot_stamp",
            "PilotStamp",
            pilot_stamps,
            allow_no_result=True,
        )

        # Custom handling, to see which pilot_stamp does not exist (if so, say which one)
        found_keys = {row["PilotStamp"] for row in results}
        missing = set(pilot_stamps) - found_keys

        if missing:
            raise PilotNotFoundError(
                data={"pilot_stamp": str(missing)},
                detail=str(missing),
                non_existing_pilots=missing,
            )

        return results

    async def get_pilot_jobs_ids_by_pilot_id(self, pilot_id: int) -> list[int]:
        """Fetch pilot jobs."""
        job_to_pilot_mapping = await fetch_records_bulk_or_raises(
            self.conn,
            JobToPilotMapping,
            PilotJobsNotFoundError,
            "pilot_id",
            "PilotID",
            [pilot_id],
            allow_more_than_one_result_per_input=True,
            allow_no_result=True,
        )

        return [mapping["JobID"] for mapping in job_to_pilot_mapping]

    async def get_pilot_ids_by_stamps(self, pilot_stamps: list[str]) -> list[int]:
        """Get pilot ids."""
        # This function is currently needed while we are relying on pilot_ids instead of pilot_stamps
        # (Ex: JobToPilotMapping)
        pilots = await self.get_pilots_by_stamp_bulk(pilot_stamps)

        return [pilot["PilotID"] for pilot in pilots]

    async def search(
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
        # TODO: Refactorize with the search function for jobs.
        # Find which columns to select
        columns = _get_columns(PilotAgents.__table__, parameters)

        stmt = select(*columns)

        stmt = apply_search_filters(
            PilotAgents.__table__.columns.__getitem__, stmt, search
        )
        stmt = apply_sort_constraints(
            PilotAgents.__table__.columns.__getitem__, stmt, sorts
        )

        if distinct:
            stmt = stmt.distinct()

        # Calculate total count before applying pagination
        total_count_subquery = stmt.alias()
        total_count_stmt = select(func.count()).select_from(total_count_subquery)
        total = (await self.conn.execute(total_count_stmt)).scalar_one()

        # Apply pagination
        if page is not None:
            if page < 1:
                raise InvalidQueryError("Page must be a positive integer")
            if per_page < 1:
                raise InvalidQueryError("Per page must be a positive integer")
            stmt = stmt.offset((page - 1) * per_page).limit(per_page)

        # Execute the query
        return total, [
            dict(row._mapping) async for row in (await self.conn.stream(stmt))
        ]

    async def clear_pilots_bulk(
        self, cutoff_date: datetime, delete_only_aborted: bool
    ) -> int:
        """Bulk delete pilots that have SubmissionTime before the 'cutoff_date'.
        Returns the number of deletion.
        """
        # TODO: Add test (Millisec?)
        stmt = delete(PilotAgents).where(PilotAgents.submission_time < cutoff_date)

        # If delete_only_aborted is True, add the condition for 'Status' being 'Aborted'
        if delete_only_aborted:
            stmt = stmt.where(PilotAgents.status == "Aborted")

        # Execute the statement
        res = await self.conn.execute(stmt)

        return res.rowcount
