from __future__ import annotations

__all__ = ["JobDB"]

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

from sqlalchemy import bindparam, case, delete, literal, select, update

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql import expression

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import JobCommand, SearchSpec, SortSpec

from ..utils import BaseSQLDB, _get_columns
from ..utils.functions import utcnow
from .schema import (
    HeartBeatLoggingInfo,
    InputData,
    JobCommands,
    JobDBBase,
    JobJDLs,
    Jobs,
)


class JobDB(BaseSQLDB):
    metadata = JobDBBase.metadata

    # Field names which should be stored in the HeartBeatLoggingInfo table
    heartbeat_fields = {
        "LoadAverage",
        "MemoryUsed",
        "Vsize",
        "AvailableDiskSpace",
        "CPUConsumed",
        "WallClockTime",
    }

    # TODO: this is copied from the DIRAC JobDB
    # but is overwritten in LHCbDIRAC, so we need
    # to find a way to make it dynamic
    jdl_2_db_parameters = ["JobName", "JobType", "JobGroup"]

    async def summary(
        self, group_by: list[str], search: list[SearchSpec]
    ) -> list[dict[str, str | int]]:
        """Get a summary of jobs aggregated by specified fields.

        Args:
            group_by: List of field names to group results by.
            search: List of search specifications to filter jobs.

        Returns:
            List of dictionaries containing grouped job statistics.

        """
        return await self._summary(table=Jobs, group_by=group_by, search=search)

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
        """Search for jobs in the database matching specified criteria.

        Args:
            parameters: List of field names to return, or None for all fields.
            search: List of search specifications to filter jobs.
            sorts: List of sort specifications for result ordering.
            distinct: If True, return only distinct results.
            per_page: Number of results per page.
            page: Page number to return, or None for all results.

        Returns:
            Tuple of (total_count, list of job dictionaries).

        """
        return await self._search(
            table=Jobs,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )

    async def create_job(self, compressed_original_jdl: str) -> int:
        """Create a new job with its original JDL.

        Args:
            compressed_original_jdl: The compressed original JDL string.

        Returns:
            The inserted job ID.

        """
        result = await self.conn.execute(
            JobJDLs.__table__.insert().values(
                JDL="",
                JobRequirements="",
                OriginalJDL=compressed_original_jdl,
            )
        )
        return result.lastrowid

    async def delete_jobs(self, job_ids: list[int]):
        """Delete jobs and their associated JDLs from the database.

        Args:
            job_ids: List of job IDs to delete.

        """
        stmt = delete(JobJDLs).where(JobJDLs.job_id.in_(job_ids))
        await self.conn.execute(stmt)

    async def insert_input_data(self, lfns: dict[int, list[str]]):
        """Insert input data LFNs for jobs.

        Args:
            lfns: Mapping of job IDs to lists of logical file names (LFNs).

        """
        await self.conn.execute(
            InputData.__table__.insert(),
            [
                {
                    "JobID": job_id,
                    "LFN": lfn,
                }
                for job_id, lfns_ in lfns.items()
                for lfn in lfns_
            ],
        )

    async def insert_job_attributes(self, jobs_to_update: dict[int, dict]):
        """Insert job attributes for newly created jobs.

        Args:
            jobs_to_update: Mapping of job IDs to their attribute dictionaries.

        """
        await self.conn.execute(
            Jobs.__table__.insert(),
            [
                {
                    "JobID": job_id,
                    **attrs,
                }
                for job_id, attrs in jobs_to_update.items()
            ],
        )

    async def update_job_jdls(self, jdls_to_update: dict[int, str]):
        """Update the JDL for existing jobs.

        Typically used just after inserting the original JDL or when rescheduling.

        Args:
            jdls_to_update: Mapping of job IDs to their compressed JDL strings.

        """
        await self.conn.execute(
            JobJDLs.__table__.update().where(
                JobJDLs.__table__.c.JobID == bindparam("b_JobID")
            ),
            [
                {
                    "b_JobID": job_id,
                    "JDL": compressed_jdl,
                }
                for job_id, compressed_jdl in jdls_to_update.items()
            ],
        )

    async def set_job_attributes(self, job_data: dict[int, dict[str, Any]]) -> None:
        """Update the parameters of the given jobs.

        Automatically updates LastUpdateTime when Status is changed.

        Args:
            job_data: Mapping of job IDs to their attribute dictionaries.

        Raises:
            ValueError: If job_data is empty.

        """
        # TODO: add myDate and force parameters.

        if not job_data:
            # nothing to do!
            raise ValueError("job_data is empty")

        for job_id in job_data.keys():
            if "Status" in job_data[job_id]:
                job_data[job_id].update(
                    {"LastUpdateTime": datetime.now(tz=timezone.utc)}
                )
        columns = set(key for attrs in job_data.values() for key in attrs.keys())
        case_expressions = {
            column: case(
                *[
                    (
                        Jobs.__table__.c.JobID == job_id,
                        # Since the setting of the new column value is obscured by the CASE statement,
                        # ensure that SQLAlchemy renders the new column value with the correct type
                        literal(attrs[column], type_=Jobs.__table__.c[column].type)
                        if not isinstance(attrs[column], expression.FunctionElement)
                        else attrs[column],
                    )
                    for job_id, attrs in job_data.items()
                    if column in attrs
                ],
                else_=getattr(Jobs.__table__.c, column),  # Retain original value
            )
            for column in columns
        }

        stmt = (
            Jobs.__table__.update()
            .values(**case_expressions)
            .where(Jobs.__table__.c.JobID.in_(job_data.keys()))
        )
        await self.conn.execute(stmt)

    async def get_job_jdls(
        self, job_ids: Iterable[int], original: bool = False
    ) -> dict[int, str]:
        """Get the JDLs for the given jobs.

        Args:
            job_ids: List of job IDs to retrieve JDLs for.
            original: If True, return the original JDL, otherwise return the processed JDL.

        Returns:
            Mapping of job IDs to their JDL strings.

        """
        if original:
            stmt = select(JobJDLs.job_id, JobJDLs.original_jdl).where(
                JobJDLs.job_id.in_(job_ids)
            )
        else:
            stmt = select(JobJDLs.job_id, JobJDLs.jdl).where(
                JobJDLs.job_id.in_(job_ids)
            )

        return {jobid: jdl for jobid, jdl in (await self.conn.execute(stmt)) if jdl}

    async def set_job_commands(self, commands: list[tuple[int, str, str]]) -> None:
        """Store commands to be passed to jobs with the next heartbeat.

        Args:
            commands: List of tuples containing (job_id, command, arguments).

        """
        await self.conn.execute(
            JobCommands.__table__.insert(),
            [
                {
                    "JobID": job_id,
                    "Command": command,
                    "Arguments": arguments,
                    "ReceptionTime": datetime.now(tz=timezone.utc),
                }
                for job_id, command, arguments in commands
            ],
        )

    async def set_properties(
        self, properties: dict[int, dict[str, Any]], update_timestamp: bool = False
    ) -> int:
        """Update job properties in bulk.

        All jobs must update the same set of properties.

        Args:
            properties: Mapping of job IDs to property dictionaries.
                Example: {job_id: {prop1: val1, prop2: val2}}.
            update_timestamp: If True, update the LastUpdateTime to now.

        Returns:
            Number of rows updated.

        Raises:
            NotImplementedError: If jobs attempt to update different sets of properties.

        """
        # Check that all we always update the same set of properties
        required_parameters_set = {tuple(sorted(k.keys())) for k in properties.values()}

        if len(required_parameters_set) != 1:
            raise NotImplementedError(
                "All the jobs should update the same set of properties"
            )

        required_parameters = list(required_parameters_set)[0]
        update_parameters = [{"job_id": k, **v} for k, v in properties.items()]

        columns = _get_columns(Jobs.__table__, required_parameters)
        values: dict[str, BindParameter[Any] | datetime] = {
            c.name: bindparam(c.name) for c in columns
        }
        if update_timestamp:
            values["LastUpdateTime"] = datetime.now(tz=timezone.utc)

        stmt = update(Jobs).where(Jobs.job_id == bindparam("job_id")).values(**values)
        rows = await self.conn.execute(stmt, update_parameters)

        return rows.rowcount

    async def add_heartbeat_data(
        self, job_id: int, dynamic_data: dict[str, str]
    ) -> None:
        """Add the job's heartbeat data to the database.

        Note:
            This does not update the HeartBeatTime column in the Jobs table.
            This is instead handled by diracx.logic.jobs.status.set_job_statuses
            as it involves updating multiple databases.

        Args:
            job_id: The job ID.
            dynamic_data: Mapping of dynamic data to store.
                Example: {"AvailableDiskSpace": "123"}.

        Raises:
            InvalidQueryError: If dynamic_data contains fields not in heartbeat_fields.

        """
        if extra_fields := set(dynamic_data) - self.heartbeat_fields:
            raise InvalidQueryError(
                f"Not allowed to store heartbeat data for: {extra_fields}. "
                f"Allowed keys are: {self.heartbeat_fields}"
            )
        values = [
            {
                "JobID": job_id,
                "Name": key,
                "Value": value,
                "HeartBeatTime": utcnow(),
            }
            for key, value in dynamic_data.items()
        ]
        await self.conn.execute(HeartBeatLoggingInfo.__table__.insert().values(values))

    async def get_job_commands(self, job_ids: Iterable[int]) -> list[JobCommand]:
        """Get commands to be passed to jobs with the next heartbeat.

        Commands are marked as "Sent" after retrieval.

        Args:
            job_ids: The job IDs to get commands for.

        Returns:
            List of JobCommand objects containing job_id, command, and arguments.

        """
        # Get the commands
        stmt = (
            select(JobCommands.job_id, JobCommands.command, JobCommands.arguments)
            .where(JobCommands.job_id.in_(job_ids), JobCommands.status == "Received")
            .order_by(JobCommands.job_id)
        )
        commands = await self.conn.execute(stmt)
        # Update the status of the commands
        stmt = (
            update(JobCommands)
            .where(JobCommands.job_id.in_(job_ids))
            .values(Status="Sent")
        )
        await self.conn.execute(stmt)
        # Return the commands grouped by job id
        return [
            JobCommand(job_id=cmd.JobID, command=cmd.Command, arguments=cmd.Arguments)
            for cmd in commands
        ]
