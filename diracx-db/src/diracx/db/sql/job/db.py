from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import BindParameter

from diracx.core.exceptions import InvalidQueryError, JobNotFound
from diracx.core.models import (
    JobStatus,
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

    @property
    def reschedule_max(self):
        # TODO: set maxRescheduling value from CS
        # maxRescheduling = self.getCSOption("MaxRescheduling", 3)
        # For now:
        return 3

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

    async def _insertNewJDL(self, jdl) -> int:
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        stmt = insert(JobJDLs).values(
            JDL="", JobRequirements="", OriginalJDL=compressJDL(jdl)
        )
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid

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

    async def insert(
        self,
        jdl,
        owner,
        owner_group,
        initial_status,
        initial_minor_status,
        vo,
    ):
        from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
        from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
            checkAndAddOwner,
            createJDLWithInitialStatus,
            fixJDL,
        )

        job_attrs = {
            "LastUpdateTime": datetime.now(tz=timezone.utc),
            "SubmissionTime": datetime.now(tz=timezone.utc),
            "Owner": owner,
            "OwnerGroup": owner_group,
            "VO": vo,
        }

        jobManifest = returnValueOrRaise(checkAndAddOwner(jdl, owner, owner_group))

        jdl = fixJDL(jdl)

        job_id = await self._insertNewJDL(jdl)

        jobManifest.setOption("JobID", job_id)

        job_attrs["JobID"] = job_id

        # 2.- Check JDL and Prepare DIRAC JDL
        jobJDL = jobManifest.dumpAsJDL()

        # Replace the JobID placeholder if any
        if jobJDL.find("%j") != -1:
            jobJDL = jobJDL.replace("%j", str(job_id))

        class_ad_job = ClassAd(jobJDL)
        class_ad_req = ClassAd("[]")
        if not class_ad_job.isOK():
            job_attrs["Status"] = JobStatus.FAILED

            job_attrs["MinorStatus"] = "Error in JDL syntax"

            await self._insertJob(job_attrs)

            return {
                "JobID": job_id,
                "Status": JobStatus.FAILED,
                "MinorStatus": "Error in JDL syntax",
            }

        class_ad_job.insertAttributeInt("JobID", job_id)

        await self.checkAndPrepareJob(
            job_id,
            class_ad_job,
            class_ad_req,
            owner,
            owner_group,
            job_attrs,
            vo,
        )

        jobJDL = createJDLWithInitialStatus(
            class_ad_job,
            class_ad_req,
            self.jdl2DBParameters,
            job_attrs,
            initial_status,
            initial_minor_status,
            modern=True,
        )

        await self.setJobJDL(job_id, jobJDL)

        # Adding the job in the Jobs table
        await self._insertJob(job_attrs)

        # TODO: check if that is actually true
        if class_ad_job.lookupAttribute("Parameters"):
            raise NotImplementedError("Parameters in the JDL are not supported")

        # Looking for the Input Data
        inputData = []
        if class_ad_job.lookupAttribute("InputData"):
            inputData = class_ad_job.getListFromExpression("InputData")
            lfns = [lfn for lfn in inputData if lfn]
            if lfns:
                await self._insertInputData(job_id, lfns)

        return {
            "JobID": job_id,
            "Status": initial_status,
            "MinorStatus": initial_minor_status,
            "TimeStamp": datetime.now(tz=timezone.utc),
        }

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
