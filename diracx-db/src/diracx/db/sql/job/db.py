"""SQL database operations for jobs, JDLs, input data, and commands."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

from sqlalchemy import bindparam, case, delete, insert, literal, select, update

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql import expression

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import JobCommand, SearchSpec, SortSpec

from ..utils import BaseSQLDB, _get_columns, utcnow
from .schema import (
    HeartBeatLoggingInfo,
    InputData,
    JobCommands,
    JobDBBase,
    JobJDLs,
    Jobs,
)


class JobDB(BaseSQLDB):
    """SQL operations for jobs.

    Attributes:
        metadata: SQLAlchemy metadata containing the job database tables.
        heartbeat_fields: Fields permitted in heartbeat logging records.
        jdl_2_db_parameters: JDL fields copied into job database records.
    """

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
        """Get a summary of the jobs.

        Args:
            group_by: Job fields used to group the summary.
            search: Search conditions to apply before summarizing.

        Returns:
            Summary rows containing grouped values and counts.
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
        """Search for jobs in the database.

        Args:
            parameters: Optional job fields to include in each result.
            search: Search conditions to apply.
            sorts: Sort specifications to apply.
            distinct: Whether to return distinct results.
            per_page: Maximum number of results per page.
            page: Optional one-based page number for pagination.

        Returns:
            Total matching job count and the requested page of job records.
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

    async def create_job(self, compressed_original_jdl: str):
        """Insert a new job with its original JDL.

        Args:
            compressed_original_jdl: Compressed original JDL content.

        Returns:
            Database identifier assigned to the inserted job.
        """
        result = await self.conn.execute(
            insert(JobJDLs).values(
                JDL="",
                JobRequirements="",
                OriginalJDL=compressed_original_jdl,
            )
        )
        return result.lastrowid

    async def delete_jobs(self, job_ids: list[int]):
        """Delete jobs and their stored JDL records.

        Args:
            job_ids: Identifiers of jobs to delete.
        """
        stmt = delete(JobJDLs).where(JobJDLs.job_id.in_(job_ids))
        await self.conn.execute(stmt)

    async def insert_input_data(self, lfns: dict[int, list[str]]):
        """Insert logical file names associated with jobs.

        Args:
            lfns: Mapping from job identifiers to input logical file names.
        """
        await self.conn.execute(
            insert(InputData),
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
        """Insert job attribute records.

        Args:
            jobs_to_update: Mapping from job identifiers to attribute values.
        """
        await self.conn.execute(
            insert(Jobs),
            [
                {
                    "JobID": job_id,
                    **attrs,
                }
                for job_id, attrs in jobs_to_update.items()
            ],
        )

    async def update_job_jdls(self, jdls_to_update: dict[int, str]):
        """Update stored JDLs, such as after insertion or rescheduling.

        Args:
            jdls_to_update: Mapping from job identifiers to compressed JDLs.
        """
        await self.conn.execute(
            update(JobJDLs).where(JobJDLs.__table__.c.JobID == bindparam("b_JobID")),
            [
                {
                    "b_JobID": job_id,
                    "JDL": compressed_jdl,
                }
                for job_id, compressed_jdl in jdls_to_update.items()
            ],
        )

    async def set_job_attributes(self, job_data):
        """Update attributes for multiple jobs.

        Args:
            job_data: Mapping from job identifiers to attribute values. When a
                status is updated, the last-update timestamp is refreshed.

        Raises:
            ValueError: If ``job_data`` is empty.
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

    async def get_job_jdls(self, job_ids, original: bool = False) -> dict[int, str]:
        """Get JDLs for the specified jobs.

        Args:
            job_ids: Identifiers of the jobs to retrieve.
            original: Whether to return original rather than current JDLs.

        Returns:
            Mapping from job identifiers to non-empty JDL content.
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
        """Store commands to deliver to jobs with their next heartbeat.

        Args:
            commands: Tuples of job identifier, command name, and arguments.
        """
        await self.conn.execute(
            insert(JobCommands),
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
        """Update the job parameters.

        All the jobs must update the same properties.

        Args:
            properties: Mapping from job identifiers to property/value mappings.
                Every job must provide the same property names.
            update_timestamp: Whether to set ``LastUpdateTime`` to now.

        Returns:
            Number of job rows updated.

        Raises:
            NotImplementedError: If jobs specify different property sets.
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

        NOTE: This does not update the HeartBeatTime column in the Jobs table.
        This is instead handled by the `diracx.logic.jobs.status.set_job_statuses`
        as it involves updating multiple databases.

        Args:
            job_id: Identifier of the job.
            dynamic_data: Heartbeat field names and values to store, such as
                ``{"AvailableDiskSpace": "123"}``.

        Raises:
            InvalidQueryError: If ``dynamic_data`` contains unsupported fields.
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
        await self.conn.execute(insert(HeartBeatLoggingInfo).values(values))

    async def get_job_commands(self, job_ids: Iterable[int]) -> list[JobCommand]:
        """Get a command to be passed to the job together with the next heartbeat.

        Args:
            job_ids: Identifiers of jobs whose pending commands should be fetched.

        Returns:
            Commands awaiting delivery, represented as ``JobCommand`` objects.
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
