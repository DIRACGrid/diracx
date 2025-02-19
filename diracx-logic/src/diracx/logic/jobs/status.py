from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
    compressJDL,
    extractJDL,
)
from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import (
    getNewStatus,
    getStartAndEndTime,
)

from diracx.core.config.schema import Config
from diracx.core.models import (
    JobLoggingRecord,
    JobMinorStatus,
    JobStatus,
    JobStatusUpdate,
    SetJobStatusReturn,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.job.db import JobDB
from diracx.db.sql.job_logging.db import JobLoggingDB
from diracx.db.sql.sandbox_metadata.db import SandboxMetadataDB
from diracx.db.sql.task_queue.db import TaskQueueDB
from diracx.logic.jobs.utils import check_and_prepare_job
from diracx.logic.task_queues.priority import recalculate_tq_shares_for_entity

logger = logging.getLogger(__name__)


async def remove_jobs(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
):
    """Fully remove a list of jobs from the WMS databases."""
    # Remove the staging task from the StorageManager
    # TODO: this was not done in the JobManagerHandler, but it was done in the kill method
    # I think it should be done here too
    # TODO: implement StorageManagerClient
    # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID([job_id]))

    # TODO: this was also  not done in the JobManagerHandler, but it was done in the JobCleaningAgent
    # I think it should be done here as well
    await sandbox_metadata_db.unassign_sandboxes_to_jobs(job_ids)

    # Remove the job from TaskQueueDB
    await remove_jobs_from_task_queue(job_ids, config, task_queue_db)

    # Remove the job from JobLoggingDB
    await job_logging_db.delete_records(job_ids)

    # Remove the job from JobDB
    await job_db.delete_jobs(job_ids)


async def set_job_statuses(
    status_changes: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    force: bool = False,
    additional_attributes: dict[int, dict[str, str]] = {},
) -> SetJobStatusReturn:
    """Set various status fields for job specified by its jobId.
    Set only the last status in the JobDB, updating all the status
    logging information in the JobLoggingDB. The status dict has datetime
    as a key and status information dictionary as values.

    :raises: JobNotFound if the job is not found in one of the DBs
    """
    # check that the datetime contains timezone info
    for job_id, status in status_changes.items():
        for dt in status:
            if dt.tzinfo is None:
                raise ValueError(
                    f"Timestamp {dt} is not timezone aware for job {job_id}"
                )

    failed: dict[int, Any] = {}
    deletable_killable_jobs = set()
    job_attribute_updates: dict[int, dict[str, str]] = {}
    job_logging_updates: list[JobLoggingRecord] = []
    status_dicts: dict[int, dict[datetime, dict[str, str]]] = defaultdict(dict)

    # transform JobStateUpdate objects into dicts
    status_dicts = {
        job_id: {
            key: {k: v for k, v in value.model_dump().items() if v is not None}
            for key, value in status.items()
        }
        for job_id, status in status_changes.items()
    }

    # search all jobs at once
    _, results = await job_db.search(
        parameters=["Status", "StartExecTime", "EndExecTime", "JobID"],
        search=[
            {
                "parameter": "JobID",
                "operator": VectorSearchOperator.IN,
                "values": list(set(status_changes.keys())),
            }
        ],
        sorts=[],
    )
    if not results:
        return SetJobStatusReturn(
            success={},
            failed={
                int(job_id): {"detail": "Not found"} for job_id in status_changes.keys()
            },
        )

    found_jobs = set(int(res["JobID"]) for res in results)
    failed.update(
        {
            int(nf_job_id): {"detail": "Not found"}
            for nf_job_id in set(status_changes.keys()) - found_jobs
        }
    )
    # Get the latest time stamps of major status updates
    wms_time_stamps = await job_logging_db.get_wms_time_stamps(found_jobs)

    for res in results:
        job_id = int(res["JobID"])
        current_status = res["Status"]
        start_time = res["StartExecTime"]
        end_time = res["EndExecTime"]

        # If the current status is Stalled and we get an update, it should probably be "Running"
        if current_status == JobStatus.STALLED:
            current_status = JobStatus.RUNNING

        #####################################################################################################
        status_dict = status_dicts[job_id]
        # This is more precise than "LastTime". time_stamps is a sorted list of tuples...
        time_stamps = sorted((float(t), s) for s, t in wms_time_stamps[job_id].items())
        last_time = TimeUtilities.fromEpoch(time_stamps[-1][0]).replace(
            tzinfo=timezone.utc
        )

        # Get chronological order of new updates
        update_times = sorted(status_dict)

        new_start_time, new_end_time = getStartAndEndTime(
            start_time, end_time, update_times, time_stamps, status_dict
        )

        job_data: dict[str, str] = {}
        new_status: str | None = None
        if update_times[-1] >= last_time:
            new_status, new_minor, new_application = (
                returnValueOrRaise(  # TODO: Catch this
                    getNewStatus(
                        job_id,
                        update_times,
                        last_time,
                        status_dict,
                        current_status,
                        force,
                        MagicMock(),  # FIXME
                    )
                )
            )

            if new_status:
                job_data.update(additional_attributes.get(job_id, {}))
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

        for upd_time in update_times:
            if status_dict[upd_time]["Source"].startswith("Job"):
                job_data["HeartBeatTime"] = str(upd_time)

        if not start_time and new_start_time:
            job_data["StartExecTime"] = new_start_time

        if not end_time and new_end_time:
            job_data["EndExecTime"] = new_end_time

        #####################################################################################################
        # delete or kill job, if we transition to DELETED or KILLED state
        if new_status in [JobStatus.DELETED, JobStatus.KILLED]:
            deletable_killable_jobs.add(job_id)

        # Update database tables
        if job_data:
            job_attribute_updates[job_id] = job_data

        for upd_time in update_times:
            s_dict = status_dict[upd_time]
            job_logging_updates.append(
                JobLoggingRecord(
                    job_id=job_id,
                    status=s_dict.get("Status", "idem"),
                    minor_status=s_dict.get("MinorStatus", "idem"),
                    application_status=s_dict.get("ApplicationStatus", "idem"),
                    date=upd_time,
                    source=s_dict.get("Source", "Unknown"),
                )
            )

    await job_db.set_job_attributes(job_attribute_updates)

    await remove_jobs_from_task_queue(
        list(deletable_killable_jobs),
        config,
        task_queue_db,
    )

    # TODO: implement StorageManagerClient
    # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID(job_ids))

    if deletable_killable_jobs:
        await job_db.set_job_commands(
            [(job_id, "Kill", "") for job_id in deletable_killable_jobs]
        )

    await job_logging_db.insert_records(job_logging_updates)

    return SetJobStatusReturn(
        success=job_attribute_updates,
        failed=failed,
    )


async def reschedule_jobs(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    reset_jobs: bool = False,
):
    """Reschedule given job."""
    failed = {}
    reschedule_max = config.Operations[
        "Defaults"
    ].Services.JobScheduling.MaxRescheduling  # type: ignore

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
            "JobID",
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

        if reset_jobs:
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

    def parse_jdl(job_id: int, job_jdl: str):
        if not job_jdl.strip().startswith("["):
            job_jdl = f"[{job_jdl}]"
        class_ad_job = ClassAd(job_jdl)
        class_ad_job.insertAttributeInt("JobID", job_id)
        return class_ad_job

    job_jdls = {
        jobid: parse_jdl(jobid, extractJDL(jdl))
        for jobid, jdl in (
            (await job_db.get_job_jdls(surviving_job_ids, original=True)).items()
        )
    }

    for job_id in surviving_job_ids:
        class_ad_job = job_jdls[job_id]
        class_ad_req = ClassAd("[]")
        try:
            await check_and_prepare_job(
                job_id,
                class_ad_job,
                class_ad_req,
                jobs_to_resched[job_id]["Owner"],
                jobs_to_resched[job_id]["OwnerGroup"],
                {"RescheduleCounter": jobs_to_resched[job_id]["RescheduleCounter"]},
                class_ad_job.getAttributeString("VirtualOrganization"),
                job_db,
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

        req_jdl = class_ad_req.asJDL()
        class_ad_job.insertAttributeInt("JobRequirements", req_jdl)
        job_jdl = class_ad_job.asJDL()
        # Replace the JobID placeholder if any
        job_jdl = job_jdl.replace("%j", str(job_id))

        additional_attrs = {
            "Site": site,
            "UserPriority": priority,
            "RescheduleTime": datetime.now(tz=timezone.utc),
            "RescheduleCounter": jobs_to_resched[job_id]["RescheduleCounter"],
        }

        # set new JDL
        jdl_changes[job_id] = compressJDL(job_jdl)

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

    if surviving_job_ids:
        set_job_status_result = await set_job_statuses(
            status_changes=status_changes,
            config=config,
            job_db=job_db,
            job_logging_db=job_logging_db,
            task_queue_db=task_queue_db,
            additional_attributes=attribute_changes,
        )

        await job_db.update_job_jdls(jdl_changes)

        success = {}
        for job_id, set_status_result in set_job_status_result.success.items():
            if job_id in failed:
                continue

            jdl = job_jdls.get(job_id, None)
            if jdl:
                jdl = jdl.asJDL()

            success[job_id] = {
                "InputData": jdl,
                **attribute_changes[job_id],
                **set_status_result.model_dump(),
            }

        return {
            "failed": failed,
            "success": success,
        }

    return {
        "success": [],
        "failed": failed,
    }


async def remove_jobs_from_task_queue(
    job_ids: list[int],
    config: Config,
    task_queue_db: TaskQueueDB,
):
    """Remove the job from TaskQueueDB."""
    await task_queue_db.remove_jobs(job_ids)

    tq_infos = await task_queue_db.get_tq_infos_for_jobs(job_ids)
    for tq_id, owner, owner_group, vo in tq_infos:
        # TODO: move to Celery

        # If the task queue is not empty, do not remove it
        if not task_queue_db.is_task_queue_empty(tq_id):
            continue

        await task_queue_db.delete_task_queue(tq_id)

        # Recalculate shares for the owner group
        await recalculate_tq_shares_for_entity(
            owner, owner_group, vo, config, task_queue_db
        )
