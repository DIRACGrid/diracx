import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

from fastapi import BackgroundTasks

from diracx.core.config.schema import Config
from diracx.core.exceptions import JobNotFound
from diracx.core.models import (
    JobMinorStatus,
    JobStatus,
    JobStatusUpdate,
    ScalarSearchOperator,
    SetJobStatusReturn,
    VectorSearchOperator,
    VectorSearchSpec,
)

from .. import JobDB, JobLoggingDB, SandboxMetadataDB, TaskQueueDB


async def reschedule_jobs_bulk(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    *,
    reset_counter=False,
) -> dict[str, Any]:
    """Reschedule given job."""
    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.Core.Utilities.ReturnValues import SErrorException

    failed = {}
    reschedule_max = (
        config.Systems.WorkloadManagement.Production.Databases.JobDB.MaxRescheduling  # type: ignore
    )

    status_changes = {}
    attribute_changes: dict[int, dict[str, str]] = defaultdict(dict)
    jdl_changes = {}

    _, results = await job_db.search(
        parameters=[
            "Status",
            "MinorStatus",
            "VerifiedFlag",
            "RescheduleCounter",
            "Owner",
            "OwnerGroup",
        ],
        search=[
            VectorSearchSpec(
                parameter="JobID", operator=VectorSearchOperator.IN, values=job_ids
            )
        ],
        sorts=[],
    )
    if not results:
        for job_id in job_ids:
            failed[job_id] = {"detail": "Not found"}

    jobs_to_resched = {}

    for job_attrs in results or []:
        job_id = int(job_attrs["JobID"])

        if "VerifiedFlag" not in job_attrs:
            failed[job_id] = {"detail": "Not found: No verified flag"}
            # Noop
            continue

        if not job_attrs["VerifiedFlag"]:
            failed[job_id] = {
                "detail": (
                    f"VerifiedFlag is False: Status {job_attrs['Status']}, "
                    f"Minor Status: {job_attrs['MinorStatus']}"
                )
            }
            # Noop
            continue

        if reset_counter:
            job_attrs["RescheduleCounter"] = 0
        else:
            job_attrs["RescheduleCounter"] = int(job_attrs["RescheduleCounter"]) + 1

        if job_attrs["RescheduleCounter"] > reschedule_max:
            status_changes[job_id] = {
                datetime.now(tz=timezone.utc): JobStatusUpdate(
                    Status=JobStatus.FAILED,
                    MinorStatus=JobMinorStatus.MAX_RESCHEDULING,
                    ApplicationStatus="Unknown",
                )
            }
            failed[job_id] = {
                "detail": f"Maximum number of reschedules exceeded ({reschedule_max})"
            }
            # DATABASE OPERATION (status change)
            continue
        jobs_to_resched[job_id] = job_attrs

    surviving_job_ids = set(jobs_to_resched.keys())

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

    def parse_jdl(job_id, job_jdl):
        if not job_jdl.strip().startswith("["):
            job_jdl = f"[{job_jdl}]"
        class_ad_job = ClassAd(job_jdl)
        class_ad_job.insertAttributeInt("JobID", job_id)
        return class_ad_job

    # DATABASE OPERATION (BULKED)
    job_jdls = {
        jobid: parse_jdl(jobid, jdl)
        for jobid, jdl in (
            (await job_db.getJobJDLs(surviving_job_ids, original=True)).items()
        )
    }

    for job_id in surviving_job_ids:
        class_ad_job = job_jdls[job_id]
        class_ad_req = ClassAd("[]")
        try:
            # NOT A DATABASE OPERATION
            await job_db.checkAndPrepareJob(
                job_id,
                class_ad_job,
                class_ad_req,
                jobs_to_resched[job_id]["Owner"],
                jobs_to_resched[job_id]["OwnerGroup"],
                {"RescheduleCounter": jobs_to_resched[job_id]["RescheduleCounter"]},
                class_ad_job.getAttributeString("VirtualOrganization"),
            )
        except SErrorException as e:
            failed[job_id] = {"detail": str(e)}
            # surviving_job_ids.remove(job_id)
            continue

        priority = class_ad_job.getAttributeInt("Priority")
        if priority is None:
            priority = 0

        site_list = class_ad_job.getListFromExpression("Site")
        if not site_list:
            site = "ANY"
        elif len(site_list) > 1:
            site = "Multiple"
        else:
            site = site_list[0]

        reqJDL = class_ad_req.asJDL()
        class_ad_job.insertAttributeInt("JobRequirements", reqJDL)
        jobJDL = class_ad_job.asJDL()
        # Replace the JobID placeholder if any
        jobJDL = jobJDL.replace("%j", str(job_id))

        additional_attrs = {
            "Site": site,
            "UserPriority": priority,
            "RescheduleTime": datetime.now(tz=timezone.utc),
            "RescheduleCounter": jobs_to_resched[job_id]["RescheduleCounter"],
        }

        # set new JDL
        jdl_changes[job_id] = jobJDL

        # set new status
        status_changes[job_id] = {
            datetime.now(tz=timezone.utc): JobStatusUpdate(
                Status=JobStatus.RECEIVED,
                MinorStatus=JobMinorStatus.RESCHEDULED,
                ApplicationStatus="Unknown",
            )
        }
        # set new attributes
        attribute_changes[job_id].update(additional_attrs)

    # BULK STATUS UPDATE
    # DATABASE OPERATION
    set_job_status_result = await set_job_statuses(
        status_changes,
        config,
        job_db,
        job_logging_db,
        task_queue_db,
        background_task,
        additional_attributes=attribute_changes,
    )

    # BULK JDL UPDATE
    # DATABASE OPERATION

    # Update JDL (Should we be doing this here?)
    # DATABASE OPERATION
    await job_db.setJobJDLsBulk(jdl_changes)

    return {
        "failed": failed,
        "success": {
            job_id: {
                "InputData": job_jdls[job_id],
                **attribute_changes[job_id],
                **set_job_status_result[job_id],
            }
        },
    }


async def set_job_statuses(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    *,
    force: bool = False,
    additional_attributes: dict[int, dict[str, str]] = {},
):
    """Bulk operation setting status on multiple job IDs, returning a dictionary of job ID to result.
    This is done by calling set_job_status for each ID and status dictionary provided within a ForgivingTaskGroup.

    """
    async with ForgivingTaskGroup() as tg:
        tasks = {
            job_id: tg.create_task(
                set_job_status(
                    job_id,
                    status_dict,
                    config,
                    job_db,
                    job_logging_db,
                    task_queue_db,
                    background_task,
                    force=force,
                    additional_attributes=additional_attributes[job_id],
                )
            )
            for job_id, status_dict in job_update.items()
        }

    return {k: v.result() for k, v in tasks.items()}


async def set_job_status(
    job_id: int,
    status: dict[datetime, JobStatusUpdate],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    *,
    force: bool = False,
    additional_attributes: dict[str, str] = {},
) -> SetJobStatusReturn:
    """Set various status fields for job specified by its jobId.
    Set only the last status in the JobDB, updating all the status
    logging information in the JobLoggingDB. The status dict has datetime
    as a key and status information dictionary as values.

    :raises: JobNotFound if the job is not found in one of the DBs
    """
    from DIRAC.Core.Utilities import TimeUtilities
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import (
        getNewStatus,
        getStartAndEndTime,
    )

    # transform JobStateUpdate objects into dicts
    statusDict = {}
    for key, value in status.items():
        statusDict[key] = {k: v for k, v in value.model_dump().items() if v is not None}

    _, res = await job_db.search(
        parameters=["Status", "StartExecTime", "EndExecTime"],
        search=[
            {
                "parameter": "JobID",
                "operator": ScalarSearchOperator.EQUAL,
                "value": str(job_id),
            }
        ],
        sorts=[],
    )
    if not res:
        raise JobNotFound(job_id) from None

    currentStatus = res[0]["Status"]
    startTime = res[0]["StartExecTime"]
    endTime = res[0]["EndExecTime"]

    # If the current status is Stalled and we get an update, it should probably be "Running"
    if currentStatus == JobStatus.STALLED:
        currentStatus = JobStatus.RUNNING

    # Get the latest time stamps of major status updates
    result = await job_logging_db.get_wms_time_stamps(job_id)

    #####################################################################################################

    # This is more precise than "LastTime". timeStamps is a sorted list of tuples...
    timeStamps = sorted((float(t), s) for s, t in result.items())
    lastTime = TimeUtilities.fromEpoch(timeStamps[-1][0]).replace(tzinfo=timezone.utc)

    # Get chronological order of new updates
    updateTimes = sorted(statusDict)

    newStartTime, newEndTime = getStartAndEndTime(
        startTime, endTime, updateTimes, timeStamps, statusDict
    )

    job_data = {}
    if updateTimes[-1] >= lastTime:
        new_status, new_minor, new_application = returnValueOrRaise(
            getNewStatus(
                job_id,
                updateTimes,
                lastTime,
                statusDict,
                currentStatus,
                force,
                MagicMock(),  # FIXME
            )
        )

        if new_status:
            job_data.update(additional_attributes)
            job_data["Status"] = new_status
            job_data["LastUpdateTime"] = str(datetime.now(timezone.utc))
        if new_minor:
            job_data["MinorStatus"] = new_minor
        if new_application:
            job_data["ApplicationStatus"] = new_application

        # TODO: implement elasticJobParametersDB ?
        # if cls.elasticJobParametersDB:
        #     result = cls.elasticJobParametersDB.setJobParameter(int(jobID), "Status", status)
        #     if not result["OK"]:
        #         return result

    for updTime in updateTimes:
        if statusDict[updTime]["Source"].startswith("Job"):
            job_data["HeartBeatTime"] = str(updTime)

    if not startTime and newStartTime:
        job_data["StartExecTime"] = newStartTime

    if not endTime and newEndTime:
        job_data["EndExecTime"] = newEndTime

    #####################################################################################################
    # delete or kill job, if we transition to DELETED or KILLED state
    if new_status in [JobStatus.DELETED, JobStatus.KILLED]:
        await _remove_jobs_from_task_queue(
            [job_id], config, task_queue_db, background_task
        )

        # TODO: implement StorageManagerClient
        # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID(job_ids))

        await job_db.set_job_command(job_id, "Kill")

    # Update database tables
    if job_data:
        await job_db.setJobAttributes(job_id, job_data)

    for updTime in updateTimes:
        sDict = statusDict[updTime]
        if not sDict.get("Status"):
            sDict["Status"] = "idem"
        if not sDict.get("MinorStatus"):
            sDict["MinorStatus"] = "idem"
        if not sDict.get("ApplicationStatus"):
            sDict["ApplicationStatus"] = "idem"
        if not sDict.get("Source"):
            sDict["Source"] = "Unknown"

        await job_logging_db.insert_record(
            job_id,
            sDict["Status"],
            sDict["MinorStatus"],
            sDict["ApplicationStatus"],
            updTime,
            sDict["Source"],
        )

    return SetJobStatusReturn(**job_data)


class ForgivingTaskGroup(asyncio.TaskGroup):
    # Hacky way, check https://stackoverflow.com/questions/75250788/how-to-prevent-python3-11-taskgroup-from-canceling-all-the-tasks
    # Basically e're using this because we want to wait for all tasks to finish, even if one of them raises an exception
    def _abort(self):
        return None


async def remove_jobs(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """Fully remove a job from the WMS databases.
    :raises: nothing.
    """
    # Remove the staging task from the StorageManager
    # TODO: this was not done in the JobManagerHandler, but it was done in the kill method
    # I think it should be done here too
    # TODO: implement StorageManagerClient
    # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID([job_id]))

    # TODO: this was also  not done in the JobManagerHandler, but it was done in the JobCleaningAgent
    # I think it should be done here as well
    await sandbox_metadata_db.unassign_sandboxes_to_jobs(job_ids)

    # Remove the job from TaskQueueDB
    await _remove_jobs_from_task_queue(job_ids, config, task_queue_db, background_task)

    # Remove the job from JobLoggingDB
    await job_logging_db.delete_records(job_ids)

    # Remove the job from JobDB
    await job_db.delete_jobs(job_ids)


async def _remove_jobs_from_task_queue(
    job_ids: list[int],
    config: Config,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """Remove the job from TaskQueueDB."""
    tq_infos = await task_queue_db.get_tq_infos_for_jobs(job_ids)
    await task_queue_db.remove_jobs(job_ids)
    for tq_id, owner, owner_group, vo in tq_infos:
        # TODO: move to Celery
        background_task.add_task(
            task_queue_db.delete_task_queue_if_empty,
            tq_id,
            owner,
            owner_group,
            config.Registry[vo].Groups[owner_group].JobShare,
            config.Registry[vo].Groups[owner_group].Properties,
            config.Operations[vo].Services.JobScheduling.EnableSharesCorrection,
            config.Registry[vo].Groups[owner_group].AllowBackgroundTQs,
        )
