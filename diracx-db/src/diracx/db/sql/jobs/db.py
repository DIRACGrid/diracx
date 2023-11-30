from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import BindParameter

from diracx.core.exceptions import InvalidQueryError, JobNotFound
from diracx.core.models import (
    JobMinorStatus,
    JobStatus,
    JobStatusReturn,
    LimitedJobStatusReturn,
    ScalarSearchOperator,
    ScalarSearchSpec,
)
from diracx.core.properties import JOB_SHARING, SecurityProperty

from ..utils import BaseSQLDB, apply_search_filters
from .schema import (
    BannedSitesQueue,
    GridCEsQueue,
    InputData,
    JobCommands,
    JobDBBase,
    JobJDLs,
    JobLoggingDBBase,
    Jobs,
    JobsQueue,
    JobTypesQueue,
    LoggingInfo,
    PlatformsQueue,
    SitesQueue,
    TagsQueue,
    TaskQueueDBBase,
    TaskQueues,
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

    # TODO: set maxRescheduling value from CS
    # maxRescheduling = self.getCSOption("MaxRescheduling", 3)
    # For now:
    maxRescheduling = 3

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        columns = _get_columns(Jobs.__table__, group_by)

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
        self,
        parameters,
        search,
        sorts,
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> list[dict[str, Any]]:
        # Find which columns to select
        columns = _get_columns(Jobs.__table__, parameters)
        stmt = select(*columns)

        stmt = apply_search_filters(Jobs.__table__, stmt, search)

        # Apply any sort constraints
        for sort in sorts:
            if sort["parameter"] not in Jobs.__table__.columns:
                raise InvalidQueryError(
                    f"Cannot sort by {sort['parameter']}: unknown column"
                )
            column = Jobs.__table__.columns[sort["parameter"]]
            if sort["direction"] == "asc":
                column = column.asc()
            elif sort["direction"] == "desc":
                column = column.desc()
            else:
                raise InvalidQueryError(f"Unknown sort {sort['direction']=}")

        if distinct:
            stmt = stmt.distinct()

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

    async def rescheduleJob(self, job_id) -> dict[str, Any]:
        """Reschedule given job"""
        from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
        from DIRAC.Core.Utilities.ReturnValues import SErrorException

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
                    "Status": JobStatus.FAILED,
                    "MinorStatus": JobMinorStatus.MAX_RESCHEDULING,
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

        # TODO: IF we keep JobParameters and OptimizerParameters: Delete job in those tables.
        # await self.delete_job_parameters(job_id)
        # await self.delete_job_optimizer_parameters(job_id)

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
        """Store a command to be passed to the job together with the next heart beat"""
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
        """
        Delete jobs from the database
        """
        stmt = delete(JobJDLs).where(JobJDLs.JobID.in_(job_ids))
        await self.conn.execute(stmt)

    async def set_properties(
        self, properties: dict[int, dict[str, Any]], update_timestamp: bool = False
    ) -> int:
        """Update the job parameters
        All the jobs must update the same properties

        :param properties: {job_id : {prop1: val1, prop2:val2}
        :param update_timestamp: if True, update the LastUpdate to now

        :return rowcount

        """

        # Check that all we always update the same set of properties
        required_parameters_set = set(
            [tuple(sorted(k.keys())) for k in properties.values()]
        )

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


MAGIC_EPOC_NUMBER = 1270000000


class JobLoggingDB(BaseSQLDB):
    """Frontend for the JobLoggingDB. Provides the ability to store changes with timestamps"""

    metadata = JobLoggingDBBase.metadata

    async def insert_record(
        self,
        job_id: int,
        status: JobStatus,
        minor_status: str,
        application_status: str,
        date: datetime,
        source: str,
    ):
        """
        Add a new entry to the JobLoggingDB table. One, two or all the three status
        components (status, minorStatus, applicationStatus) can be specified.
        Optionally the time stamp of the status can
        be provided in a form of a string in a format '%Y-%m-%d %H:%M:%S' or
        as datetime.datetime object. If the time stamp is not provided the current
        UTC time is used.
        """

        # First, fetch the maximum SeqNum for the given job_id
        seqnum_stmt = select(func.coalesce(func.max(LoggingInfo.SeqNum) + 1, 1)).where(
            LoggingInfo.JobID == job_id
        )
        seqnum = await self.conn.scalar(seqnum_stmt)

        epoc = (
            time.mktime(date.timetuple())
            + date.microsecond / 1000000.0
            - MAGIC_EPOC_NUMBER
        )

        stmt = insert(LoggingInfo).values(
            JobID=int(job_id),
            SeqNum=seqnum,
            Status=status,
            MinorStatus=minor_status,
            ApplicationStatus=application_status[:255],
            StatusTime=date,
            StatusTimeOrder=epoc,
            Source=source[:32],
        )
        await self.conn.execute(stmt)

    async def get_records(self, job_id: int) -> list[JobStatusReturn]:
        """Returns a Status,MinorStatus,ApplicationStatus,StatusTime,Source tuple
        for each record found for job specified by its jobID in historical order
        """

        stmt = (
            select(
                LoggingInfo.Status,
                LoggingInfo.MinorStatus,
                LoggingInfo.ApplicationStatus,
                LoggingInfo.StatusTime,
                LoggingInfo.Source,
            )
            .where(LoggingInfo.JobID == int(job_id))
            .order_by(LoggingInfo.StatusTimeOrder, LoggingInfo.StatusTime)
        )
        rows = await self.conn.execute(stmt)

        values = []
        for (
            status,
            minor_status,
            application_status,
            status_time,
            status_source,
        ) in rows:
            values.append(
                [
                    status,
                    minor_status,
                    application_status,
                    status_time.replace(tzinfo=timezone.utc),
                    status_source,
                ]
            )

        # If no value has been set for the application status in the first place,
        # We put this status to unknown
        res = []
        if values:
            if values[0][2] == "idem":
                values[0][2] = "Unknown"

            # We replace "idem" values by the value previously stated
            for i in range(1, len(values)):
                for j in range(3):
                    if values[i][j] == "idem":
                        values[i][j] = values[i - 1][j]

            # And we replace arrays with tuples
            for (
                status,
                minor_status,
                application_status,
                status_time,
                status_source,
            ) in values:
                res.append(
                    JobStatusReturn(
                        Status=status,
                        MinorStatus=minor_status,
                        ApplicationStatus=application_status,
                        StatusTime=status_time,
                        Source=status_source,
                    )
                )

        return res

    async def delete_records(self, job_ids: list[int]):
        """Delete logging records for given jobs"""
        stmt = delete(LoggingInfo).where(LoggingInfo.JobID.in_(job_ids))
        await self.conn.execute(stmt)

    async def get_wms_time_stamps(self, job_id):
        """Get TimeStamps for job MajorState transitions
        return a {State:timestamp} dictionary
        """

        result = {}
        stmt = select(
            LoggingInfo.Status,
            LoggingInfo.StatusTimeOrder,
        ).where(LoggingInfo.JobID == job_id)
        rows = await self.conn.execute(stmt)
        if not rows.rowcount:
            raise JobNotFound(job_id) from None

        for event, etime in rows:
            result[event] = str(etime + MAGIC_EPOC_NUMBER)

        return result


class TaskQueueDB(BaseSQLDB):
    metadata = TaskQueueDBBase.metadata

    async def get_tq_infos_for_jobs(
        self, job_ids: list[int]
    ) -> set[tuple[int, str, str, str]]:
        """
        Get the task queue info for given jobs
        """
        stmt = select(
            TaskQueues.TQId, TaskQueues.Owner, TaskQueues.OwnerGroup, TaskQueues.VO
        ).where(JobsQueue.JobId.in_(job_ids))
        return set(
            (int(row[0]), str(row[1]), str(row[2]), str(row[3]))
            for row in (await self.conn.execute(stmt)).all()
        )

    async def get_owner_for_task_queue(self, tq_id: int) -> dict[str, str]:
        """
        Get the owner and owner group for a task queue
        """
        stmt = select(TaskQueues.Owner, TaskQueues.OwnerGroup, TaskQueues.VO).where(
            TaskQueues.TQId == tq_id
        )
        return dict((await self.conn.execute(stmt)).one()._mapping)

    async def remove_job(self, job_id: int):
        """
        Remove a job from the task queues
        """
        stmt = delete(JobsQueue).where(JobsQueue.JobId == job_id)
        await self.conn.execute(stmt)

    async def remove_jobs(self, job_ids: list[int]):
        """
        Remove jobs from the task queues
        """
        stmt = delete(JobsQueue).where(JobsQueue.JobId.in_(job_ids))
        await self.conn.execute(stmt)

    async def delete_task_queue_if_empty(
        self,
        tq_id: int,
        tq_owner: str,
        tq_group: str,
        job_share: int,
        group_properties: set[SecurityProperty],
        enable_shares_correction: bool,
        allow_background_tqs: bool,
    ):
        """
        Try to delete a task queue if it's empty
        """
        # Check if the task queue is empty
        stmt = (
            select(TaskQueues.TQId)
            .where(TaskQueues.Enabled >= 1)
            .where(TaskQueues.TQId == tq_id)
            .where(~TaskQueues.TQId.in_(select(JobsQueue.TQId)))
        )
        rows = await self.conn.execute(stmt)
        if not rows.rowcount:
            return

        # Deleting the task queue (the other tables will be deleted in cascade)
        stmt = delete(TaskQueues).where(TaskQueues.TQId == tq_id)
        await self.conn.execute(stmt)

        await self.recalculate_tq_shares_for_entity(
            tq_owner,
            tq_group,
            job_share,
            group_properties,
            enable_shares_correction,
            allow_background_tqs,
        )

    async def recalculate_tq_shares_for_entity(
        self,
        owner: str,
        group: str,
        job_share: int,
        group_properties: set[SecurityProperty],
        enable_shares_correction: bool,
        allow_background_tqs: bool,
    ):
        """
        Recalculate the shares for a user/userGroup combo
        """
        if JOB_SHARING in group_properties:
            # If group has JobSharing just set prio for that entry, user is irrelevant
            return await self.__set_priorities_for_entity(
                owner, group, job_share, group_properties, allow_background_tqs
            )

        stmt = (
            select(TaskQueues.Owner, func.count(TaskQueues.Owner))
            .where(TaskQueues.OwnerGroup == group)
            .group_by(TaskQueues.Owner)
        )
        rows = await self.conn.execute(stmt)
        # make the rows a list of tuples
        # Get owners in this group and the amount of times they appear
        # TODO: I guess the rows are already a list of tupes
        # maybe refactor
        data = [(r[0], r[1]) for r in rows if r]
        numOwners = len(data)
        # If there are no owners do now
        if numOwners == 0:
            return
        # Split the share amongst the number of owners
        entities_shares = {row[0]: job_share / numOwners for row in data}

        # TODO: implement the following
        # If corrector is enabled let it work it's magic
        # if enable_shares_correction:
        #     entities_shares = await self.__shares_corrector.correct_shares(
        #         entitiesShares, group=group
        #     )

        # Keep updating
        owners = dict(data)
        # IF the user is already known and has more than 1 tq, the rest of the users don't need to be modified
        # (The number of owners didn't change)
        if owner in owners and owners[owner] > 1:
            await self.__set_priorities_for_entity(
                owner,
                group,
                entities_shares[owner],
                group_properties,
                allow_background_tqs,
            )
            return
        # Oops the number of owners may have changed so we recalculate the prio for all owners in the group
        for owner in owners:
            await self.__set_priorities_for_entity(
                owner,
                group,
                entities_shares[owner],
                group_properties,
                allow_background_tqs,
            )

    async def __set_priorities_for_entity(
        self,
        owner: str,
        group: str,
        share,
        properties: set[SecurityProperty],
        allow_background_tqs: bool,
    ):
        """
        Set the priority for a user/userGroup combo given a splitted share
        """
        from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import calculate_priority

        stmt = (
            select(
                TaskQueues.TQId,
                func.sum(JobsQueue.RealPriority) / func.count(JobsQueue.RealPriority),
            )
            .join(JobsQueue, TaskQueues.TQId == JobsQueue.TQId)
            .where(TaskQueues.OwnerGroup == group)
            .group_by(TaskQueues.TQId)
        )
        if JOB_SHARING not in properties:
            stmt = stmt.where(TaskQueues.Owner == owner)
        rows = await self.conn.execute(stmt)
        tq_dict: dict[int, float] = {tq_id: priority for tq_id, priority in rows}

        if not tq_dict:
            return

        rows = await self.retrieve_task_queues(list(tq_dict))

        prio_dict = calculate_priority(tq_dict, rows, share, allow_background_tqs)

        # Execute updates
        for prio, tqs in prio_dict.items():
            update_stmt = (
                update(TaskQueues).where(TaskQueues.TQId.in_(tqs)).values(Priority=prio)
            )
            await self.conn.execute(update_stmt)

    async def retrieve_task_queues(self, tq_id_list=None):
        """
        Get all the task queues
        """
        if tq_id_list is not None and not tq_id_list:
            # Empty list => Fast-track no matches
            return {}

        stmt = (
            select(
                TaskQueues.TQId,
                TaskQueues.Priority,
                func.count(JobsQueue.TQId).label("Jobs"),
                TaskQueues.Owner,
                TaskQueues.OwnerGroup,
                TaskQueues.VO,
                TaskQueues.CPUTime,
            )
            .join(JobsQueue, TaskQueues.TQId == JobsQueue.TQId)
            .join(SitesQueue, TaskQueues.TQId == SitesQueue.TQId)
            .join(GridCEsQueue, TaskQueues.TQId == GridCEsQueue.TQId)
            .group_by(
                TaskQueues.TQId,
                TaskQueues.Priority,
                TaskQueues.Owner,
                TaskQueues.OwnerGroup,
                TaskQueues.VO,
                TaskQueues.CPUTime,
            )
        )
        if tq_id_list is not None:
            stmt = stmt.where(TaskQueues.TQId.in_(tq_id_list))

        tq_data: dict[int, dict[str, list[str]]] = dict(
            dict(row._mapping) for row in await self.conn.execute(stmt)
        )
        # TODO: the line above should be equivalent to the following commented code, check this is the case
        # for record in rows:
        #     tqId = record[0]
        #     tqData[tqId] = {
        #         "Priority": record[1],
        #         "Jobs": record[2],
        #         "Owner": record[3],
        #         "OwnerGroup": record[4],
        #         "VO": record[5],
        #         "CPUTime": record[6],
        #     }

        for tq_id in tq_data:
            # TODO: maybe factorize this handy tuple list
            for table, field in {
                (SitesQueue, "Sites"),
                (GridCEsQueue, "GridCEs"),
                (BannedSitesQueue, "BannedSites"),
                (PlatformsQueue, "Platforms"),
                (JobTypesQueue, "JobTypes"),
                (TagsQueue, "Tags"),
            }:
                stmt = select(table.Value).where(table.TQId == tq_id)
                tq_data[tq_id][field] = list(
                    row[0] for row in await self.conn.execute(stmt)
                )

        return tq_data
