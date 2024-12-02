from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import BindParameter

from diracx.core.exceptions import InvalidQueryError, JobNotFoundError
from diracx.core.models import (
    JobMinorStatus,
    JobStatus,
    LimitedJobStatusReturn,
    ScalarSearchOperator,
    ScalarSearchSpec,
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
    jdl_2_db_parameters = ["JobName", "JobType", "JobGroup"]

    # TODO: set max_rescheduling value from CS
    # max_rescheduling = self.getCSOption("MaxRescheduling", 3)
    # For now:
    max_rescheduling = 3

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

    async def _insert_new_jdl(self, jdl) -> int:
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        stmt = insert(JobJDLs).values(
            JDL="", JobRequirements="", OriginalJDL=compressJDL(jdl)
        )
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid

    async def _insert_job(self, job_data: dict[str, Any]):
        stmt = insert(Jobs).values(job_data)
        await self.conn.execute(stmt)

    async def _insert_input_data(self, job_id: int, lfns: list[str]):
        stmt = insert(InputData).values([{"JobID": job_id, "LFN": lfn} for lfn in lfns])
        await self.conn.execute(stmt)

    async def set_job_attributes(self, job_id, job_data):
        """TODO: add myDate and force parameters."""
        if "Status" in job_data:
            job_data = job_data | {"LastUpdateTime": datetime.now(tz=timezone.utc)}
        stmt = update(Jobs).where(Jobs.JobID == job_id).values(job_data)
        await self.conn.execute(stmt)

    async def _check_and_prepare_job(
        self,
        job_id,
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

        ret_val = checkAndPrepareJob(
            job_id,
            class_ad_job,
            class_ad_req,
            owner,
            owner_group,
            job_attrs,
            vo,
        )

        if not ret_val["OK"]:
            if cmpError(ret_val, EWMSSUBM):
                await self.set_job_attributes(job_id, job_attrs)

            returnValueOrRaise(ret_val)

    async def set_job_jdl(self, job_id, jdl):
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        stmt = (
            update(JobJDLs).where(JobJDLs.JobID == job_id).values(JDL=compressJDL(jdl))
        )
        await self.conn.execute(stmt)

    async def get_job_jdl(self, job_id: int, original: bool = False) -> str:
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

        job_manifest = returnValueOrRaise(checkAndAddOwner(jdl, owner, owner_group))

        jdl = fixJDL(jdl)

        job_id = await self._insert_new_jdl(jdl)

        job_manifest.setOption("JobID", job_id)

        job_attrs["JobID"] = job_id

        # 2.- Check JDL and Prepare DIRAC JDL
        job_jdl = job_manifest.dumpAsJDL()

        # Replace the JobID placeholder if any
        if job_jdl.find("%j") != -1:
            job_jdl = job_jdl.replace("%j", str(job_id))

        class_ad_job = ClassAd(job_jdl)
        class_ad_req = ClassAd("[]")
        if not class_ad_job.isOK():
            job_attrs["Status"] = JobStatus.FAILED

            job_attrs["MinorStatus"] = "Error in JDL syntax"

            await self._insert_job(job_attrs)

            return {
                "JobID": job_id,
                "Status": JobStatus.FAILED,
                "MinorStatus": "Error in JDL syntax",
            }

        class_ad_job.insertAttributeInt("JobID", job_id)

        await self._check_and_prepare_job(
            job_id,
            class_ad_job,
            class_ad_req,
            owner,
            owner_group,
            job_attrs,
            vo,
        )

        job_jdl = createJDLWithInitialStatus(
            class_ad_job,
            class_ad_req,
            self.jdl_2_db_parameters,
            job_attrs,
            initial_status,
            initial_minor_status,
            modern=True,
        )

        await self.set_job_jdl(job_id, job_jdl)

        # Adding the job in the Jobs table
        await self._insert_job(job_attrs)

        # TODO: check if that is actually true
        if class_ad_job.lookupAttribute("Parameters"):
            raise NotImplementedError("Parameters in the JDL are not supported")

        # Looking for the Input Data
        input_data = []
        if class_ad_job.lookupAttribute("InputData"):
            input_data = class_ad_job.getListFromExpression("InputData")
            lfns = [lfn for lfn in input_data if lfn]
            if lfns:
                await self._insert_input_data(job_id, lfns)

        return {
            "JobID": job_id,
            "Status": initial_status,
            "MinorStatus": initial_minor_status,
            "TimeStamp": datetime.now(tz=timezone.utc),
        }

    async def reschedule_job(self, job_id) -> dict[str, Any]:
        """Reschedule given job."""
        from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
        from DIRAC.Core.Utilities.ReturnValues import SErrorException

        _, result = await self.search(
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

        job_attrs = result[0]

        if "VerifiedFlag" not in job_attrs:
            raise ValueError(f"Job {job_id} not found in the system")

        if not job_attrs["VerifiedFlag"]:
            raise ValueError(
                f"Job {job_id} not Verified: Status {job_attrs['Status']}, Minor Status: {job_attrs['MinorStatus']}"
            )

        reschedule_counter = int(job_attrs["RescheduleCounter"]) + 1

        # TODO: update max_rescheduling:
        # self.max_rescheduling = self.getCSOption("MaxRescheduling", self.max_rescheduling)

        if reschedule_counter > self.max_rescheduling:
            logging.warning(
                f"Job {job_id}: Maximum number of reschedulings is reached."
            )
            self.set_job_attributes(
                job_id,
                {
                    "Status": JobStatus.FAILED,
                    "MinorStatus": JobMinorStatus.MAX_RESCHEDULING,
                },
            )
            raise ValueError(
                f"Maximum number of reschedulings is reached: {self.max_rescheduling}"
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

        # TODO: IF we keep JobParameters and OptimizerParameters: Delete job in those tables.
        # await self.delete_job_parameters(job_id)
        # await self.delete_job_optimizer_parameters(job_id)

        job_jdl = await self.get_job_jdl(job_id, original=True)
        if not job_jdl.strip().startswith("["):
            job_jdl = f"[{job_jdl}]"

        class_ad_job = ClassAd(job_jdl)
        class_ad_req = ClassAd("[]")
        ret_val = {}
        ret_val["JobID"] = job_id

        class_ad_job.insertAttributeInt("JobID", job_id)

        try:
            result = await self._check_and_prepare_job(
                job_id,
                class_ad_job,
                class_ad_req,
                job_attrs["Owner"],
                job_attrs["OwnerGroup"],
                new_job_attributes,
                class_ad_job.getAttributeString("VirtualOrganization"),
            )
        except SErrorException as e:
            raise ValueError(e) from e

        priority = class_ad_job.getAttributeInt("Priority")
        if priority is None:
            priority = 0
        job_attrs["UserPriority"] = priority

        site_list = class_ad_job.getListFromExpression("Site")
        if not site_list:
            site = "ANY"
        elif len(site_list) > 1:
            site = "Multiple"
        else:
            site = site_list[0]

        job_attrs["Site"] = site

        job_attrs["Status"] = JobStatus.RECEIVED

        job_attrs["MinorStatus"] = JobMinorStatus.RESCHEDULED

        job_attrs["ApplicationStatus"] = "Unknown"

        job_attrs["LastUpdateTime"] = datetime.now(tz=timezone.utc)

        job_attrs["RescheduleTime"] = datetime.now(tz=timezone.utc)

        req_jdl = class_ad_req.asJDL()
        class_ad_job.insertAttributeInt("JobRequirements", req_jdl)

        job_jdl = class_ad_job.asJDL()

        # Replace the JobID placeholder if any
        job_jdl = job_jdl.replace("%j", str(job_id))

        result = await self.set_job_jdl(job_id, job_jdl)

        result = await self.set_job_attributes(job_id, job_attrs)

        ret_val["InputData"] = class_ad_job.lookupAttribute("InputData")
        ret_val["RescheduleCounter"] = reschedule_counter
        ret_val["Status"] = JobStatus.RECEIVED
        ret_val["MinorStatus"] = JobMinorStatus.RESCHEDULED

        return ret_val

    async def get_job_status(self, job_id: int) -> LimitedJobStatusReturn:
        try:
            stmt = select(Jobs.Status, Jobs.MinorStatus, Jobs.ApplicationStatus).where(
                Jobs.JobID == job_id
            )
            return LimitedJobStatusReturn(
                **dict((await self.conn.execute(stmt)).one()._mapping)
            )
        except NoResultFound as e:
            raise JobNotFoundError(job_id) from e

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
            raise JobNotFoundError(job_id) from e

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
