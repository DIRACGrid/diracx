from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, func, insert, select, update

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import ScalarSearchOperator, ScalarSearchSpec
from diracx.core.utils import JobStatus

from ..utils import BaseDB, apply_search_filters
from .schema import Base as JobDBBase
from .schema import InputData, JobJDLs, JobParameters, Jobs, OptimizerParameters


class JobDB(BaseDB):
    # This needs to be here for the BaseDB to create the engine
    metadata = JobDBBase.metadata

    # TODO: this is copied from the DIRAC JobDB
    # but is overwriten in LHCbDIRAC, so we need
    # to find a way to make it dynamic
    jdl2DBParameters = ["JobName", "JobType", "JobGroup"]

    # TODO: set maxRescheduling value from CS
    # maxRescheduling = self.getCSOption("MaxRescheduling", 3)
    # For now:
    maxRescheduling = 3

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        columns = [Jobs.__table__.columns[x] for x in group_by]

        stmt = select(*columns, func.count(Jobs.JobID).label("count"))
        stmt = apply_search_filters(Jobs.__table__, stmt, search)
        stmt = stmt.group_by(*columns)

        # Execute the query
        return [
            dict(row._mapping)
            async for row in (await self.conn.stream(stmt))
            if row.count > 0  # type: ignore
        ]

    async def search(
        self, parameters, search, sorts, *, per_page: int = 100, page: int | None = None
    ) -> list[dict[str, Any]]:
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
        for sort in sorts:
            column = Jobs.__table__.columns[sort["parameter"]]
            if sort["direction"] == "asc":
                column = column.asc()
            elif sort["direction"] == "desc":
                column = column.desc()
            else:
                raise InvalidQueryError(f"Unknown sort {sort['direction']=}")

        # Apply pagination
        if page:
            raise NotImplementedError("TODO Not yet implemented")

        # Execute the query
        return [dict(row._mapping) async for row in (await self.conn.stream(stmt))]

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
        owner_group,
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
            job_attrs["Status"] = JobStatus.Failed

            job_attrs["MinorStatus"] = "Error in JDL syntax"

            await self._insertJob(job_attrs)

            return {
                "JobID": job_id,
                "Status": JobStatus.Failed,
                "MinorStatus": "Error in JDL syntax",
            }

        class_ad_job.insertAttributeInt("JobID", job_id)

        await self._checkAndPrepareJob(
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

    async def delete_job_parameters(self, job_id: int):
        stmt = delete(JobParameters).where(JobParameters.JobID == job_id)
        await self.conn.execute(stmt)

    async def delete_job_optimizer_parameters(self, job_id: int):
        stmt = delete(OptimizerParameters).where(OptimizerParameters.JobID == job_id)
        await self.conn.execute(stmt)

    async def rescheduleJob(self, job_id) -> dict[str, Any]:
        """Reschedule given job"""
        from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
        from DIRAC.Core.Utilities.ReturnValues import SErrorException
        from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus

        result = await self.search(
            parameters=[
                "Status",
                "MinorStatus",
                "VerifiedFlag",
                "RescheduleCounter",
                "Owner",
                "OwnerGroup",
            ],
            search=[
                ScalarSearchSpec(
                    parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=job_id
                )
            ],
            sorts=[],
        )
        if not result:
            raise ValueError(f"Job {job_id} not found.")

        jobAttrs = result[0]

        if "VerifiedFlag" not in jobAttrs:
            raise ValueError(f"Job {job_id} not found in the system")

        if not jobAttrs["VerifiedFlag"]:
            raise ValueError(
                f"Job {job_id} not Verified: Status {jobAttrs['Status']}, Minor Status: {jobAttrs['MinorStatus']}"
            )

        reschedule_counter = int(jobAttrs["RescheduleCounter"]) + 1

        # TODO: update maxRescheduling:
        # self.maxRescheduling = self.getCSOption("MaxRescheduling", self.maxRescheduling)

        if reschedule_counter > self.maxRescheduling:
            logging.warn(f"Job {job_id}: Maximum number of reschedulings is reached.")
            self.setJobAttributes(
                job_id,
                {
                    "Status": JobStatus.Failed,
                    "MinorStatus": "Maximum of reschedulings reached",
                },
            )
            raise ValueError(
                f"Maximum number of reschedulings is reached: {self.maxRescheduling}"
            )

        new_job_attributes = {"RescheduleCounter": reschedule_counter}

        # TODO: get the job parameters from JobMonitoringClient
        # result = JobMonitoringClient().getJobParameters(jobID)
        # if result["OK"]:
        #     parDict = result["Value"]
        #     for key, value in parDict.get(jobID, {}).items():
        #         result = self.setAtticJobParameter(jobID, key, value, rescheduleCounter - 1)
        #         if not result["OK"]:
        #             break

        # Delete job in JobParameters and OptimizerParameters:
        await self.delete_job_parameters(job_id)
        await self.delete_job_optimizer_parameters(job_id)

        job_jdl = await self.getJobJDL(job_id, original=True)
        if not job_jdl.strip().startswith("["):
            job_jdl = f"[{job_jdl}]"

        classAdJob = ClassAd(job_jdl)
        classAdReq = ClassAd("[]")
        retVal = {}
        retVal["JobID"] = job_id

        classAdJob.insertAttributeInt("JobID", job_id)

        try:
            result = self._checkAndPrepareJob(
                job_id,
                classAdJob,
                classAdReq,
                jobAttrs["Owner"],
                jobAttrs["OwnerGroup"],
                # TODO: OwnerDN doesn't exist anymore
                # set empty string
                "",
                # TODO: DiracSetup doesn't exist anymore
                # set empty string
                "",
                new_job_attributes,
                classAdJob.getAttributeString("VirtualOrganization"),
            )
        except SErrorException as e:
            raise ValueError(e) from e

        priority = classAdJob.getAttributeInt("Priority")
        if priority is None:
            priority = 0
        jobAttrs["UserPriority"] = priority

        siteList = classAdJob.getListFromExpression("Site")
        if not siteList:
            site = "ANY"
        elif len(siteList) > 1:
            site = "Multiple"
        else:
            site = siteList[0]

        jobAttrs["Site"] = site

        jobAttrs["Status"] = JobStatus.RECEIVED

        jobAttrs["MinorStatus"] = JobMinorStatus.RESCHEDULED

        jobAttrs["ApplicationStatus"] = "Unknown"

        jobAttrs["ApplicationNumStatus"] = 0

        jobAttrs["LastUpdateTime"] = str(datetime.utcnow())

        jobAttrs["RescheduleTime"] = str(datetime.utcnow())

        reqJDL = classAdReq.asJDL()
        classAdJob.insertAttributeInt("JobRequirements", reqJDL)

        jobJDL = classAdJob.asJDL()

        # Replace the JobID placeholder if any
        jobJDL = jobJDL.replace("%j", str(job_id))

        result = self.setJobJDL(job_id, jobJDL)

        result = self.setJobAttributes(job_id, jobAttrs)

        retVal["InputData"] = classAdJob.lookupAttribute("InputData")
        retVal["RescheduleCounter"] = reschedule_counter
        retVal["Status"] = JobStatus.RECEIVED
        retVal["MinorStatus"] = JobMinorStatus.RESCHEDULED

        return retVal
