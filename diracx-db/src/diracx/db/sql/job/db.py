from __future__ import annotations

from asyncio import TaskGroup
from copy import deepcopy
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import bindparam, delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import BindParameter
from diracx.core.exceptions import InvalidQueryError, JobNotFound
from diracx.core.models import (
    LimitedJobStatusReturn,
    SearchSpec,
    SortSpec,
)

from ..utils import BaseSQLDB, apply_search_filters, apply_sort_constraints
from .schema import (
    InputData,
    JobCommands,
    JobDBBase,
    JobJDLs,
    Jobs,
)


class JobSubmissionSpec(BaseModel):
    jdl: str
    owner: str
    owner_group: str
    initial_status: str
    initial_minor_status: str
    vo: str


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

    # TODO: this is copied from the DIRAC JobDB
    # but is overwriten in LHCbDIRAC, so we need
    # to find a way to make it dynamic
    jdl2DBParameters = ["JobName", "JobType", "JobGroup"]

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        columns = _get_columns(Jobs.__table__, group_by)

        stmt = select(*columns, func.count(Jobs.JobID).label("count"))
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

    async def _insertJob(self, jobData: dict[str, Any]):
        stmt = insert(Jobs).values(jobData)
        await self.conn.execute(stmt)

    async def _insertInputData(self, job_id: int, lfns: list[str]):
        stmt = insert(InputData).values([{"JobID": job_id, "LFN": lfn} for lfn in lfns])
        await self.conn.execute(stmt)

    async def setJobAttributes(self, job_id, jobData):
        """TODO: add myDate and force parameters."""
        if "Status" in jobData:
            jobData = jobData | {"LastUpdateTime": datetime.now(tz=timezone.utc)}
        stmt = update(Jobs).where(Jobs.JobID == job_id).values(jobData)
        await self.conn.execute(stmt)

    async def checkAndPrepareJob(
        self,
        jobID,
        class_ad_job,
        class_ad_req,
        owner,
        owner_group,
        job_attrs,
        vo,
    ):
        """Check Consistency of Submitted JDL and set some defaults
        Prepare subJDL with Job Requirements.
        """
        from DIRAC.Core.Utilities.DErrno import EWMSSUBM, cmpError
        from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
            checkAndPrepareJob,
        )

        retVal = checkAndPrepareJob(
            jobID,
            class_ad_job,
            class_ad_req,
            owner,
            owner_group,
            job_attrs,
            vo,
        )

        if not retVal["OK"]:
            if cmpError(retVal, EWMSSUBM):
                await self.setJobAttributes(jobID, job_attrs)

            returnValueOrRaise(retVal)

    async def setJobJDL(self, job_id, jdl):
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        stmt = (
            update(JobJDLs).where(JobJDLs.JobID == job_id).values(JDL=compressJDL(jdl))
        )
        await self.conn.execute(stmt)

    async def setJobJDLsBulk(self, jdls):
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        await self.conn.execute(
            JobJDLs.__table__.update().where(
                JobJDLs.__table__.c.JobID == bindparam("b_JobID")
            ),
            [{"b_JobID": jid, "JDL": compressJDL(jdl)} for jid, jdl in jdls.items()],
        )

    async def setJobAttributesBulk(self, jobData):
        """TODO: add myDate and force parameters."""
        for job_id in jobData.keys():
            if "Status" in jobData[job_id]:
                jobData[job_id].update(
                    {"LastUpdateTime": datetime.now(tz=timezone.utc)}
                )

        await self.conn.execute(
            Jobs.__table__.update().where(
                Jobs.__table__.c.JobID == bindparam("b_JobID")
            ),
            [{"b_JobID": job_id, **attrs} for job_id, attrs in jobData.items()],
        )

    async def getJobJDL(self, job_id: int, original: bool = False) -> str:
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import extractJDL

        if original:
            stmt = select(JobJDLs.OriginalJDL).where(JobJDLs.JobID == job_id)
        else:
            stmt = select(JobJDLs.JDL).where(JobJDLs.JobID == job_id)

        jdl = (await self.conn.execute(stmt)).scalar_one()
        if jdl:
            jdl = extractJDL(jdl)

        return jdl

    async def getJobJDLs(self, job_ids, original: bool = False) -> dict[int | str, str]:
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import extractJDL

        if original:
            stmt = select(JobJDLs.JobID, JobJDLs.OriginalJDL).where(
                JobJDLs.JobID.in_(job_ids)
            )
        else:
            stmt = select(JobJDLs.JobID, JobJDLs.JDL).where(JobJDLs.JobID.in_(job_ids))

        return {
            jobid: extractJDL(jdl)
            for jobid, jdl in (await self.conn.execute(stmt))
            if jdl
        }

    async def insert_bulk(
        self,
        jobs: list[JobSubmissionSpec],
    ):
        from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
        from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
            checkAndAddOwner,
            compressJDL,
            createJDLWithInitialStatus,
        )

        jobs_to_insert = []
        jdls_to_update = []
        inputdata_to_insert = []
        original_jdls = []

        # generate the jobIDs first
        async with TaskGroup() as tg:
            for job in jobs:
                original_jdl = deepcopy(job.jdl)
                jobManifest = returnValueOrRaise(
                    checkAndAddOwner(original_jdl, job.owner, job.owner_group)
                )

                # Fix possible lack of brackets
                if original_jdl.strip()[0] != "[":
                    original_jdl = f"[{original_jdl}]"

                original_jdls.append(
                    (
                        original_jdl,
                        jobManifest,
                        tg.create_task(
                            self.conn.execute(
                                JobJDLs.__table__.insert().values(
                                    JDL="",
                                    JobRequirements="",
                                    OriginalJDL=compressJDL(original_jdl),
                                )
                            )
                        ),
                    )
                )

        job_ids = []

        async with TaskGroup() as tg:
            for job, (original_jdl, jobManifest_, job_id_task) in zip(
                jobs, original_jdls
            ):
                job_id = job_id_task.result().lastrowid
                job_attrs = {
                    "JobID": job_id,
                    "LastUpdateTime": datetime.now(tz=timezone.utc),
                    "SubmissionTime": datetime.now(tz=timezone.utc),
                    "Owner": job.owner,
                    "OwnerGroup": job.owner_group,
                    "VO": job.vo,
                }

                jobManifest_.setOption("JobID", job_id)

                # 2.- Check JDL and Prepare DIRAC JDL
                jobJDL = jobManifest_.dumpAsJDL()

                # Replace the JobID placeholder if any
                if jobJDL.find("%j") != -1:
                    jobJDL = jobJDL.replace("%j", str(job_id))

                class_ad_job = ClassAd(jobJDL)

                class_ad_req = ClassAd("[]")
                if not class_ad_job.isOK():
                    # Rollback the entire transaction
                    raise ValueError(f"Error in JDL syntax for job JDL: {original_jdl}")
                # TODO: check if that is actually true
                if class_ad_job.lookupAttribute("Parameters"):
                    raise NotImplementedError("Parameters in the JDL are not supported")

                # TODO is this even needed?
                class_ad_job.insertAttributeInt("JobID", job_id)

                await self.checkAndPrepareJob(
                    job_id,
                    class_ad_job,
                    class_ad_req,
                    job.owner,
                    job.owner_group,
                    job_attrs,
                    job.vo,
                )
                jobJDL = createJDLWithInitialStatus(
                    class_ad_job,
                    class_ad_req,
                    self.jdl2DBParameters,
                    job_attrs,
                    job.initial_status,
                    job.initial_minor_status,
                    modern=True,
                )
                # assert "JobType" in job_attrs, job_attrs
                job_ids.append(job_id)
                jobs_to_insert.append(job_attrs)
                jdls_to_update.append(
                    {
                        "b_JobID": job_id,
                        "JDL": compressJDL(jobJDL),
                    }
                )

                if class_ad_job.lookupAttribute("InputData"):
                    inputData = class_ad_job.getListFromExpression("InputData")
                    inputdata_to_insert += [
                        {"JobID": job_id, "LFN": lfn} for lfn in inputData if lfn
                    ]

            tg.create_task(
                self.conn.execute(
                    JobJDLs.__table__.update().where(
                        JobJDLs.__table__.c.JobID == bindparam("b_JobID")
                    ),
                    jdls_to_update,
                )
            )
            tg.create_task(
                self.conn.execute(
                    Jobs.__table__.insert(),
                    jobs_to_insert,
                )
            )

            if inputdata_to_insert:
                tg.create_task(
                    self.conn.execute(
                        InputData.__table__.insert(),
                        inputdata_to_insert,
                    )
                )

        return job_ids

    async def insert(
        self,
        jdl,
        owner,
        owner_group,
        initial_status,
        initial_minor_status,
        vo,
    ):
        submitted_job_ids = await self.insert_bulk(
            [
                JobSubmissionSpec(
                    jdl=jdl,
                    owner=owner,
                    owner_group=owner_group,
                    initial_status=initial_status,
                    initial_minor_status=initial_minor_status,
                    vo=vo,
                )
            ]
        )

        return submitted_job_ids[0]

    async def get_job_status(self, job_id: int) -> LimitedJobStatusReturn:
        try:
            stmt = select(Jobs.Status, Jobs.MinorStatus, Jobs.ApplicationStatus).where(
                Jobs.JobID == job_id
            )
            return LimitedJobStatusReturn(
                **dict((await self.conn.execute(stmt)).one()._mapping)
            )
        except NoResultFound as e:
            raise JobNotFound(job_id) from e

    async def set_job_command(self, job_id: int, command: str, arguments: str = ""):
        """Store a command to be passed to the job together with the next heart beat."""
        try:
            stmt = insert(JobCommands).values(
                JobID=job_id,
                Command=command,
                Arguments=arguments,
                ReceptionTime=datetime.now(tz=timezone.utc),
            )
            await self.conn.execute(stmt)
        except IntegrityError as e:
            raise JobNotFound(job_id) from e

    async def set_job_command_bulk(self, commands):
        """Store a command to be passed to the job together with the next heart beat."""
        self.conn.execute(
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
        # FIXME handle IntegrityError

    async def delete_jobs(self, job_ids: list[int]):
        """Delete jobs from the database."""
        stmt = delete(JobJDLs).where(JobJDLs.JobID.in_(job_ids))
        await self.conn.execute(stmt)

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

        stmt = update(Jobs).where(Jobs.JobID == bindparam("job_id")).values(**values)
        rows = await self.conn.execute(stmt, update_parameters)

        return rows.rowcount
