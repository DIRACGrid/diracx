import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock

from fastapi import BackgroundTasks

from diracx.core.config.schema import Config
from diracx.core.exceptions import JobNotFoundError
from diracx.core.models import (
    JobStatus,
    JobStatusUpdate,
    ScalarSearchOperator,
    SetJobStatusReturn,
)

from .. import JobDB, JobLoggingDB, SandboxMetadataDB, TaskQueueDB


async def set_job_status(
    job_id: int,
    status: dict[datetime, JobStatusUpdate],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    force: bool = False,
) -> SetJobStatusReturn:
    """Set various status fields for job specified by its jobId.
    Set only the last status in the JobDB, updating all the status
    logging information in the JobLoggingDB. The status dict has datetime
    as a key and status information dictionary as values.

    :raises: JobNotFoundError if the job is not found in one of the DBs
    """
    from DIRAC.Core.Utilities import TimeUtilities
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import (
        getNewStatus,
        getStartAndEndTime,
    )

    # transform JobStateUpdate objects into dicts
    status_dict = {}
    for key, value in status.items():
        status_dict[key] = {
            k: v for k, v in value.model_dump().items() if v is not None
        }

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
        raise JobNotFoundError(job_id) from None

    current_status = res[0]["Status"]
    start_time = res[0]["StartExecTime"]
    end_time = res[0]["EndExecTime"]

    # If the current status is Stalled and we get an update, it should probably be "Running"
    if current_status == JobStatus.STALLED:
        current_status = JobStatus.RUNNING

    # Get the latest time stamps of major status updates
    result = await job_logging_db.get_wms_time_stamps(job_id)

    #####################################################################################################

    # This is more precise than "LastTime". time_stamps is a sorted list of tuples...
    time_stamps = sorted((float(t), s) for s, t in result.items())
    last_time = TimeUtilities.fromEpoch(time_stamps[-1][0]).replace(tzinfo=timezone.utc)

    # Get chronological order of new updates
    update_times = sorted(status_dict)

    new_start_time, new_end_time = getStartAndEndTime(
        start_time, end_time, update_times, time_stamps, status_dict
    )

    job_data = {}
    if update_times[-1] >= last_time:
        new_status, new_minor, new_application = returnValueOrRaise(
            getNewStatus(
                job_id,
                update_times,
                last_time,
                status_dict,
                current_status,
                force,
                MagicMock(),
            )
        )

        if new_status:
            job_data["Status"] = new_status
            job_data["LastUpdateTime"] = datetime.now(timezone.utc)
        if new_minor:
            job_data["MinorStatus"] = new_minor
        if new_application:
            job_data["ApplicationStatus"] = new_application

        # TODO: implement elasticJobParametersDB ?
        # if cls.elasticJobParametersDB:
        #     result = cls.elasticJobParametersDB.setJobParameter(int(jobID), "Status", status)
        #     if not result["OK"]:
        #         return result

    for upd_time in update_times:
        if status_dict[upd_time]["Source"].startswith("Job"):
            job_data["HeartBeatTime"] = upd_time

    if not start_time and new_start_time:
        job_data["StartExecTime"] = new_start_time

    if not end_time and new_end_time:
        job_data["EndExecTime"] = new_end_time

    if job_data:
        await job_db.set_job_attributes(job_id, job_data)

    for upd_time in update_times:
        s_dict = status_dict[upd_time]
        if not s_dict.get("Status"):
            s_dict["Status"] = "idem"
        if not s_dict.get("MinorStatus"):
            s_dict["MinorStatus"] = "idem"
        if not s_dict.get("ApplicationStatus"):
            s_dict["ApplicationStatus"] = "idem"
        if not s_dict.get("Source"):
            s_dict["Source"] = "Unknown"

        await job_logging_db.insert_record(
            job_id,
            s_dict["Status"],
            s_dict["MinorStatus"],
            s_dict["ApplicationStatus"],
            upd_time,
            s_dict["Source"],
        )

    return SetJobStatusReturn(**job_data)


class ForgivingTaskGroup(asyncio.TaskGroup):
    # Hacky way, check https://stackoverflow.com/questions/75250788/how-to-prevent-python3-11-taskgroup-from-canceling-all-the-tasks
    # Basically e're using this because we want to wait for all tasks to finish, even if one of them raises an exception
    def _abort(self):
        return None


async def delete_jobs(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """Removing jobs from task queues, send a kill command and set status to DELETED.

    :raises: BaseExceptionGroup[JobNotFoundError] for every job that was not found.
    """
    await _remove_jobs_from_task_queue(job_ids, config, task_queue_db, background_task)
    # TODO: implement StorageManagerClient
    # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID(job_ids))

    async with ForgivingTaskGroup() as task_group:
        for job_id in job_ids:
            task_group.create_task(job_db.set_job_command(job_id, "Kill"))

            task_group.create_task(
                set_job_status(
                    job_id,
                    {
                        datetime.now(timezone.utc): JobStatusUpdate(
                            Status=JobStatus.DELETED,
                            MinorStatus="Checking accounting",
                            Source="job_manager",
                        )
                    },
                    job_db,
                    job_logging_db,
                    force=True,
                )
            )


async def kill_jobs(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """Kill jobs by removing them from the task queues, set kill as a job command and setting the job status to KILLED.
    :raises: BaseExceptionGroup[JobNotFoundError] for every job that was not found.
    """
    await _remove_jobs_from_task_queue(job_ids, config, task_queue_db, background_task)
    # TODO: implement StorageManagerClient
    # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID(job_ids))

    async with ForgivingTaskGroup() as task_group:
        for job_id in job_ids:
            task_group.create_task(job_db.set_job_command(job_id, "Kill"))
            task_group.create_task(
                set_job_status(
                    job_id,
                    {
                        datetime.now(timezone.utc): JobStatusUpdate(
                            Status=JobStatus.KILLED,
                            MinorStatus="Marked for termination",
                            Source="job_manager",
                        )
                    },
                    job_db,
                    job_logging_db,
                    force=True,
                )
            )

    # TODO: Consider using the code below instead, probably more stable but less performant
    # errors = []
    # for job_id in job_ids:
    #     try:
    #         await job_db.set_job_command(job_id, "Kill")
    #         await set_job_status(
    #             job_id,
    #             {
    #                 datetime.now(timezone.utc): JobStatusUpdate(
    #                     Status=JobStatus.KILLED,
    #                     MinorStatus="Marked for termination",
    #                     Source="job_manager",
    #                 )
    #             },
    #             job_db,
    #             job_logging_db,
    #             force=True,
    #         )
    #     except JobNotFoundError as e:
    #         errors.append(e)

    # if errors:
    #     raise BaseExceptionGroup("Some job ids were not found", errors)


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
