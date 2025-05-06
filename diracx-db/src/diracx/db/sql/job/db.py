from __future__ import annotations

__all__ = ["JobDB"]

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

from sqlalchemy import bindparam, case, delete, func, insert, select, text, update

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import BindParameter

from DIRAC.WorkloadManagementSystem.Client import JobStatus

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import JobCommand, SearchSpec, SortSpec

from ..utils import BaseSQLDB, apply_search_filters, apply_sort_constraints
from ..utils.functions import utcnow
from .schema import (
    HeartBeatLoggingInfo,
    InputData,
    JobCommands,
    JobDBBase,
    JobJDLs,
    Jobs,
)


def _get_columns(table, parameters):
    columns = [x for x in table.columns]
    if parameters:
        if unrecognised_parameters := set(parameters) - set(table.columns.keys()):
            raise InvalidQueryError(
                f"Unrecognised parameters requested {unrecognised_parameters}"
            )
        columns = [c for c in columns if c.name in parameters]
    return columns


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

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        """Get a summary of the jobs."""
        columns = _get_columns(Jobs.__table__, group_by)

        stmt = select(*columns, func.count(Jobs.job_id).label("count"))
        stmt = apply_search_filters(Jobs.__table__.columns.__getitem__, stmt, search)
        stmt = stmt.group_by(*columns)

        # Execute the query
        return [
            dict(row._mapping)
            async for row in (await self.conn.stream(stmt))
            if row.count > 0  # type: ignore
        ]

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
        """Search for jobs in the database."""
        # Find which columns to select
        columns = _get_columns(Jobs.__table__, parameters)

        stmt = select(*columns)

        stmt = apply_search_filters(Jobs.__table__.columns.__getitem__, stmt, search)
        stmt = apply_sort_constraints(Jobs.__table__.columns.__getitem__, stmt, sorts)

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

    async def create_job(self, compressed_original_jdl: str):
        """Used to insert a new job with original JDL. Returns inserted job id."""
        result = await self.conn.execute(
            JobJDLs.__table__.insert().values(
                JDL="",
                JobRequirements="",
                OriginalJDL=compressed_original_jdl,
            )
        )
        return result.lastrowid

    async def delete_jobs(self, job_ids: list[int]):
        """Delete jobs from the database."""
        stmt = delete(JobJDLs).where(JobJDLs.job_id.in_(job_ids))
        await self.conn.execute(stmt)

    async def insert_input_data(self, lfns: dict[int, list[str]]):
        """Insert input data for jobs."""
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
        """Insert the job attributes."""
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
        """Used to update the JDL, typically just after inserting the original JDL, or rescheduling, for example."""
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

    @staticmethod
    def _set_job_attributes_fix_value(column, value):
        """Apply corrections to the values before inserting them into the database.

        TODO: Move this logic into the sqlalchemy model.
        """
        if column == "VerifiedFlag":
            value_str = str(value)
            if value_str in ("True", "False"):
                return value_str
        if column == "AccountedFlag":
            value_str = str(value)
            if value_str in ("True", "False", "Failed"):
                return value_str
        else:
            return value
        raise NotImplementedError(f"Unrecognized value for column {column}: {value}")

    async def set_job_attributes(self, job_data):
        """Update the parameters of the given jobs."""
        # TODO: add myDate and force parameters.
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
                        self._set_job_attributes_fix_value(column, attrs[column]),
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
        """Get the JDLs for the given jobs."""
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
        """Store a command to be passed to the job together with the next heart beat."""
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
        """Update the job parameters
        All the jobs must update the same properties.

        :param properties: {job_id : {prop1: val1, prop2:val2}
        :param update_timestamp: if True, update the LastUpdate to now

        :return rowcount

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

        :param job_id: the job id
        :param dynamic_data: mapping of the dynamic data to store,
            e.g. {"AvailableDiskSpace": 123}
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
        """Get a command to be passed to the job together with the next heartbeat.

        :param job_ids: the job ids
        :return: mapping of job id to list of commands
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

    async def fill_jobs_history_summary(self):
        """Fill the JobsHistorySummary table with the summary of the jobs in a final state."""
        # Create the staging table
        dialect = self.conn.dialect
        if dialect == "mysql":
            create_staging_table_sql = "CREATE TABLE IF NOT EXISTS JobsHistorySummary_staging LIKE JobsHistorySummary"
        elif dialect == "postgresql":
            create_staging_table_sql = (
                "CREATE TABLE IF NOT EXISTS JobsHistorySummary_staging "
                "(LIKE JobsHistorySummary INCLUDING ALL)"
            )
        elif dialect == "sqlite":
            create_staging_table_sql = "CREATE TABLE IF NOT EXISTS JobsHistorySummary_staging AS JobsHistorySummary"
        await self.conn.execute(text(create_staging_table_sql))

        if dialect == "mysql":
            current_date_expr = "UTC_DATE()"
        elif dialect == "postgresql":
            current_date_expr = "CURRENT_DATE"
        elif dialect == "sqlite":
            current_date_expr = "DATE('now')"
        else:
            raise ValueError(f"Unsupported DB dialect: {dialect}")

        # Columns for grouping
        def_columns = "Status, Site, Owner, OwnerGroup, JobGroup, JobType, ApplicationStatus, MinorStatus"
        agg_columns = "COUNT(JobID), SUM(RescheduleCounter)"

        # Final states list
        final_states = JobStatus.JOB_FINAL_STATES + JobStatus.JOB_REALLY_FINAL_STATES
        final_states_sql = ", ".join(f"'{state}'" for state in final_states)

        # Build SQL statement
        insert_sql = f"""
            INSERT INTO JobsHistorySummary_staging
            SELECT {def_columns}, {agg_columns}
            FROM Jobs
            WHERE Status IN ({final_states_sql})
            AND LastUpdateTime < {current_date_expr}
            GROUP BY {def_columns}
        """  # noqa: S608
        await self.conn.execute(text(insert_sql))

        stmts = []

        if dialect in {"mysql", "sqlite", "postgresql"}:
            stmts = [
                "ALTER TABLE JobsHistorySummary RENAME TO JobsHistorySummary_old;",
                "ALTER TABLE JobsHistorySummary_staging RENAME TO JobsHistorySummary;",
                "DROP TABLE JobsHistorySummary_old;",
            ]
        else:
            raise ValueError(f"Unsupported DB dialect: {dialect}")

        for stmt in stmts:
            await self.conn.execute(text(stmt))
