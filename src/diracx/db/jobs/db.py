from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, insert, select, update

from diracx.core.exceptions import InvalidQueryError
from diracx.core.utils import JobStatus

from ..utils import BaseDB
from .schema import Base as JobDBBase
from .schema import InputData, JobJDLs, Jobs


def apply_search_filters(table, stmt, search):
    # Apply any filters
    for column, operator, value in search:
        column = table.columns[column]
        if operator == "eq":
            expr = column == value
        elif operator == "neq":
            expr = column != value
        elif operator == "gt":
            expr = column > value
        elif operator == "lt":
            expr = column < value
        elif operator in "in":
            expr = column.in_(value)
        elif operator in "like":
            expr = column.like(value)
        else:
            raise InvalidQueryError(f"Unknown filter {operator=}")
        stmt = stmt.where(expr)
    return stmt


class JobDB(BaseDB):
    # This needs to be here for the BaseDB to create the engine
    metadata = JobDBBase.metadata

    # TODO: this is copied from the DIRAC JobDB
    # but is overwriten in LHCbDIRAC, so we need
    # to find a way to make it dynamic
    jdl2DBParameters = ["JobName", "JobType", "JobGroup"]

    async def summary(self, group_by, search):
        columns = [Jobs.__table__.columns[x] for x in group_by]

        stmt = select(*columns, func.count(Jobs.JobID).label("count"))
        stmt = apply_search_filters(Jobs.__table__, stmt, search)
        stmt = stmt.group_by(*columns)

        # Execute the query
        return [
            row._mapping
            async for row in (await self.conn.stream(stmt))
            if row.count > 0
        ]

    async def search(
        self, parameters, search, sort, *, per_page: int = 100, page: int | None = None
    ):
        # Find which columns to select
        columns = [x for x in Jobs.__table__.columns]
        if parameters:
            if unrecognised_parameters := set(parameters) - set(
                Jobs.__table__.columns.keys()
            ):
                raise InvalidQueryError(
                    f"Unrecognised parameters requested {unrecognised_parameters}"
                )
            columns = [c for c in columns if c.name in parameters]
        stmt = select(*columns)

        stmt = apply_search_filters(Jobs.__table__, stmt, search)

        # Apply any sort constraints
        for column, direction in sort:
            column = Jobs.__table__.columns[column]
            if direction == "asc":
                column = column.asc()
            elif direction == "desc":
                column = column.desc()
            else:
                raise InvalidQueryError(f"Unknown sort {direction=}")

        # Apply pagination
        if page is not None:
            raise NotImplementedError("TODO Not yet implemented")

        # Execute the query
        return [row._mapping async for row in (await self.conn.stream(stmt))]

    async def list(self):
        stmt = select(JobJDLs)
        res = [row._mapping async for row in (await self.conn.stream(stmt))]
        # return
        return res
        # result = await self.conn.execute(stmt)
        # return result.fetchall()

    async def _insertNewJDL(self, jdl) -> int:
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        stmt = insert(JobJDLs).values(
            JDL="", JobRequirements="", OriginalJDL=compressJDL(jdl)
        )
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid

    async def _insertJob(self, jobData: dict):
        stmt = insert(Jobs).values(jobData)
        await self.conn.execute(stmt)

    async def _insertInputData(self, job_id: int, lfns: list):
        stmt = insert(InputData).values([{"JobID": job_id, "LFN": lfn} for lfn in lfns])
        await self.conn.execute(stmt)

    async def setJobAttributes(self, job_id, jobData):
        """
        TODO: add myDate and force parameters
        """
        if "Status" in jobData:
            jobData = jobData | {"LastUpdateTime": datetime.now(tz=timezone.utc)}
        stmt = update(Jobs).where(Jobs.JobID == job_id).values(jobData)
        await self.conn.execute(stmt)

    async def _checkAndPrepareJob(
        self,
        jobID,
        class_ad_job,
        class_ad_req,
        owner,
        owner_dn,
        owner_group,
        dirac_setup,
        job_attrs,
        vo,
    ):
        """
        Check Consistency of Submitted JDL and set some defaults
        Prepare subJDL with Job Requirements
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
            owner_dn,
            owner_group,
            dirac_setup,
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

    async def insert(
        self,
        jdl,
        owner,
        owner_dn,
        owner_group,
        dirac_setup,
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
            "OwnerDN": owner_dn,
            "OwnerGroup": owner_group,
            "DIRACSetup": dirac_setup,
        }

        jobManifest = returnValueOrRaise(
            checkAndAddOwner(jdl, owner, owner_dn, owner_group, dirac_setup)
        )

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
            job_attrs["Status"] = JobStatus.Failed

            job_attrs["MinorStatus"] = "Error in JDL syntax"

            await self._insertJob(job_attrs)

            return {
                "JobID": job_id,
                "Status": JobStatus.FAILED,
                "MinorStatus": "Error in JDL syntax",
            }

        class_ad_job.insertAttributeInt("JobID", job_id)

        await self._checkAndPrepareJob(
            job_id,
            class_ad_job,
            class_ad_req,
            owner,
            owner_dn,
            owner_group,
            dirac_setup,
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


async def get_job_db():
    async with JobDB() as job_db:
        yield job_db
