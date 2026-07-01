"""Job status handling and transition helpers for DIRACX.

This module implements job status updates, status-driven cleanup, and
integration with task queues, job logging, and sandbox metadata for DIRACX
job lifecycle management.
"""

from __future__ import annotations

import logging
from asyncio import TaskGroup
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable
from unittest.mock import MagicMock

from DIRACCommon.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRACCommon.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import (
    compressJDL,
    extractJDL,
)
from DIRACCommon.WorkloadManagementSystem.Utilities.JobStatusUtility import (
    getNewStatus,
    getStartAndEndTime,
)

from diracx.core.config import Config
from diracx.core.models import (
    HeartbeatData,
    JobAttributes,
    JobCommand,
    JobLoggingRecord,
    JobMetaData,
    JobMinorStatus,
    JobParameters,
    JobStatus,
    JobStatusUpdate,
    SetJobStatusReturn,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.os.job_parameters import JobParametersDB
from diracx.db.sql.job.db import JobDB
from diracx.db.sql.job_logging.db import JobLoggingDB
from diracx.db.sql.sandbox_metadata.db import SandboxMetadataDB
from diracx.db.sql.task_queue.db import TaskQueueDB
from diracx.db.sql.utils.functions import utcnow
from diracx.logic.task_queues import recalculate_tq_shares_for_entity

from .utils import check_and_prepare_job

logger = logging.getLogger(__name__)

# Create alias mappings for performance
JOB_ATTRIBUTES_ALIASES = {
    field.alias: field_name
    for field_name, field in JobAttributes.model_fields.items()
    if field.alias
}
JOB_PARAMETERS_ALIASES = {
    field.alias: field_name
    for field_name, field in JobParameters.model_fields.items()
    if field.alias
}


async def remove_jobs(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
):
    """Permanently remove jobs and related records from WMS databases.

    This operation performs a best-effort cleanup of all data associated
    with the provided job identifiers. The procedure includes:
    - Unassigning any sandboxes associated with the jobs.
    - Removing entries from task queues that reference the jobs.
    - Deleting logging records for the jobs from the JobLoggingDB.
    - Deleting the jobs themselves from the JobDB.

    Args:
        job_ids (list[int]): List of job IDs to remove.
        config (Config): Application configuration used for VO-specific
            behavior (unused currently but provided for symmetry with other
            job operations).
        job_db (JobDB): Database accessor for job records; used to delete
            the jobs.
        job_logging_db (JobLoggingDB): Database accessor for job logging
            records; used to delete logging entries for the jobs.
        sandbox_metadata_db (SandboxMetadataDB): Accessor used to unassign
            sandboxes associated with the jobs.
        task_queue_db (TaskQueueDB): Accessor for task queues; used to
            remove tasks related to the jobs.

    Returns:
        None

    Raises:
        Exceptions raised by any of the database accessors may propagate
        (e.g. connectivity or integrity errors).
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
    job_parameters_db: JobParametersDB,
    force: bool = False,
    additional_attributes: dict[int, dict[str, str]] = {},
) -> SetJobStatusReturn:
    """Apply status updates for multiple jobs and persist changes.

    Processes a mapping of job identifiers to time-keyed status updates
    and applies them to the WMS. For each job the function:
    - Computes derived fields (Start/End execution times) and determines
      the new canonical job status using the DIRAC utilities.
    - Updates job attributes in the JobDB and job parameters if needed.
    - Appends structured logging records to the JobLoggingDB.
    - Schedules delete/kill commands for transitions to DELETED or KILLED.
    - Removes affected jobs from task queues as required.

    Args:
        status_changes (dict[int, dict[datetime, JobStatusUpdate]]): A map
            from job ID to a mapping of timestamps to ``JobStatusUpdate``
            objects describing status changes.
        config (Config): Application configuration used to evaluate VO
            specific behaviour.
        job_db (JobDB): Database accessor for job records.
        job_logging_db (JobLoggingDB): Database accessor for job logging
            records.
        task_queue_db (TaskQueueDB): Database accessor for task queues; used
            to remove or update queued tasks for affected jobs.
        job_parameters_db (JobParametersDB): Accessor used to persist job
            parameter updates derived from status changes.
        force (bool): When true, forces status transitions even if they
            would otherwise be suppressed by business logic.
        additional_attributes (dict[int, dict[str, str]]): Optional mapping
            of job-specific attribute updates to apply alongside status
            changes.

    Returns:
        SetJobStatusReturn: Object containing two mappings: ``success`` with
            applied updates per job and ``failed`` with error details for
            jobs that could not be updated.

    Raises:
        ValueError: If any provided timestamp is not timezone-aware.
        Exceptions from underlying DB accessors or DIRAC utilities may
            propagate (e.g. connectivity errors or unexpected API failures).
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
    skipped_job_attribute_updates: set[int] = set()
    job_logging_updates: list[JobLoggingRecord] = []
    status_dicts: dict[int, dict[datetime, dict[str, str]]] = defaultdict(dict)

    # transform JobStateUpdate objects into dicts
    status_dicts = {
        job_id: {
            key: {
                k: v
                for k, v in value.model_dump(by_alias=True).items()
                if v is not None
            }
            for key, value in status.items()
        }
        for job_id, status in status_changes.items()
    }

    # search all jobs at once
    _, results = await job_db.search(
        parameters=["Status", "StartExecTime", "EndExecTime", "JobID", "VO"],
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
        # time_stamps = sorted((float(t), s) for s, t in wms_time_stamps[job_id].items())
        first_status = min(
            wms_time_stamps[job_id].items(), key=lambda x: x[1], default=("", 0)
        )[0]
        last_time = max(wms_time_stamps[job_id].values())

        # Get chronological order of new updates
        update_times = sorted(status_dict)

        new_start_time, new_end_time = getStartAndEndTime(
            start_time,
            end_time,
            update_times,
            # Use a type ignore hint here as it exists solely to use the DIRAC API
            defaultdict(lambda x=first_status: x),  # type: ignore[misc]
            status_dict,
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

            await job_parameters_db.upsert(res["VO"], job_id, {"Status": new_status})

        for upd_time in update_times:
            source = status_dict[upd_time]["Source"]
            if source.startswith("Job") or source == "Heartbeat":
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
        else:
            skipped_job_attribute_updates.add(job_id)

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

    if job_attribute_updates:
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
        success=job_attribute_updates | {j: {} for j in skipped_job_attribute_updates},
        failed=failed,
    )


async def reschedule_jobs(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
    reset_jobs: bool = False,
):
    """Reschedule one or more jobs according to the VO's scheduling policy.

    For each provided job ID the function validates that the job is
    reschedulable (has a verified flag, has not exceeded the maximum
    reschedule count) and prepares a fresh scheduling request.
    If ``reset_jobs`` is true the reschedule counter for matching jobs is
    reset to zero; otherwise the counter is incremented. Jobs that exceed
    the configured maximum rescheduling threshold are marked as failed with
    a suitable ``JobStatusUpdate`` placed in the returned failure mapping.

    Args:
        job_ids (list[int]): List of job IDs to reschedule.
        config (Config): Application configuration used to obtain VO-level
            scheduling limits and behavior.
        job_db (JobDB): Database accessor for job records and JDL retrieval.
        job_logging_db (JobLoggingDB): Database accessor for job logging; may
            be used to append reschedule-related log entries.
        task_queue_db (TaskQueueDB): Accessor for task queues; used to
            manipulate queued tasks during rescheduling.
        job_parameters_db (JobParametersDB): Accessor for job parameter
            storage; used to persist updates to parameters like ``Status``.
        reset_jobs (bool): If true reset the reschedule counter instead of
            incrementing it.

    Returns:
        None

    Notes:
        - The function performs multiple DB reads and writes; database
          exceptions may propagate to the caller.
        - The exact resubmission mechanism uses DIRAC ClassAd/JDL helpers
          and integrates with the job preparation utilities in this module.
    """
    failed = {}
    status_changes = {}
    attribute_changes: defaultdict[int, dict[str, str]] = defaultdict(dict)
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
            "VO",
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

        reschedule_max = config.operations[
            job_attrs["VO"]
        ].services.job_scheduling.max_rescheduling

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

    for job_id, job_attrs in jobs_to_resched.items():
        class_ad_job = job_jdls[job_id]
        class_ad_req = ClassAd("[]")
        try:
            await check_and_prepare_job(
                job_id,
                class_ad_job,
                class_ad_req,
                job_attrs["Owner"],
                job_attrs["OwnerGroup"],
                {"RescheduleCounter": job_attrs["RescheduleCounter"]},
                job_attrs["VO"],
                job_db,
                config,
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
            "RescheduleCounter": job_attrs["RescheduleCounter"],
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

    success = {}
    if surviving_job_ids:
        set_job_status_result = await set_job_statuses(
            status_changes=status_changes,
            config=config,
            job_db=job_db,
            job_logging_db=job_logging_db,
            task_queue_db=task_queue_db,
            job_parameters_db=job_parameters_db,
            additional_attributes=attribute_changes,
        )

        await job_db.update_job_jdls(jdl_changes)

        for job_id, set_status_result in set_job_status_result.success.items():
            if job_id in failed:
                continue

            jdl = job_jdls.get(job_id, None)
            if jdl:
                jdl = jdl.asJDL()

            success[job_id] = {
                "InputData": jdl,
                **attribute_changes[job_id],
                **set_status_result.model_dump(by_alias=True),
            }

    return {"failed": failed, "success": success}


async def remove_jobs_from_task_queue(
    job_ids: list[int],
    config: Config,
    task_queue_db: TaskQueueDB,
):
    """Remove job entries from task queues and cleanup empty queues.

    This helper removes references to the specified jobs from the task
    queue database. After removing the jobs it inspects any task queues
    that referenced those jobs; if a task queue becomes empty it is
    deleted and the shares for the owner group are recalculated.

    Args:
        job_ids (list[int]): List of job IDs to remove from task queues.
        config (Config): Application configuration used when recalculating
            task-queue shares for an entity.
        task_queue_db (TaskQueueDB): Database accessor for task-queue
            operations; used to remove job references, query queue
            information, delete empty queues, and check queue occupancy.

    Returns:
        None

    Raises:
        Exceptions from the task queue database accessor may propagate
        (e.g. connectivity errors). Note: some steps are TODO and may be
        migrated to asynchronous workers (e.g. Celery) in the future.
    """
    await task_queue_db.remove_jobs(job_ids)

    tq_infos = await task_queue_db.get_tq_infos_for_jobs(job_ids)
    for tq_id, owner, owner_group, vo in tq_infos:
        # TODO: move to Celery

        # If the task queue is not empty, do not remove it
        if not await task_queue_db.is_task_queue_empty(tq_id):
            continue

        await task_queue_db.delete_task_queue(tq_id)

        # Recalculate shares for the owner group
        await recalculate_tq_shares_for_entity(
            owner, owner_group, vo, config, task_queue_db
        )


async def set_job_parameters_or_attributes(
    updates: dict[int, JobMetaData],
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
):
    """Set job attributes and/or parameters for multiple jobs.

    The function accepts a mapping from job id to a ``JobMetaData`` model.
    For each metadata object it separates fields into job attributes (DB
    fields) and job parameters (stored in the JobParametersDB).
    Fields that match a
    known job attribute alias are applied via a bulk ``set_job_attributes``
    call; fields that match a job parameter alias are upserted into the
    ``JobParametersDB``. Unknown fields are treated as parameters to allow
    flexible metadata handling.

    Args:
        updates (dict[int, JobMetaData]): Mapping of job ID to metadata
            describing attributes and parameters to set.
        job_db (JobDB): Database accessor used to persist job attribute
            updates.
        job_parameters_db (JobParametersDB): Database accessor used to
            upsert job parameters.

    Returns:
        None

    Raises:
        Exceptions raised by the underlying database accessors may propagate
        (e.g. connectivity errors).
    """
    # Those dicts create a mapping of job_id -> {attribute_name: value}
    attr_updates: dict[int, dict[str, Any]] = {}
    param_updates: dict[int, dict[str, Any]] = {}

    for job_id, metadata in updates.items():
        attr_updates[job_id] = {}
        param_updates[job_id] = {}
        for pname, pvalue in metadata.model_dump(
            by_alias=True, exclude_none=True
        ).items():
            # An argument can be a job attribute and/or a job parameter

            # Check if the argument is a valid job attribute (using alias)
            if pname in JOB_ATTRIBUTES_ALIASES:
                attr_updates[job_id][pname] = pvalue

            # Check if the argument is a valid job parameter (using alias)
            if pname in JOB_PARAMETERS_ALIASES:
                param_updates[job_id][pname] = pvalue

            # If the field is not in either known aliases, default to treating it as a parameter
            # This allows for more flexible metadata handling
            elif pname not in JOB_ATTRIBUTES_ALIASES:
                param_updates[job_id][pname] = pvalue

    # Bulk set job attributes if required
    attr_updates = {k: v for k, v in attr_updates.items() if v}
    if attr_updates:
        await job_db.set_job_attributes(attr_updates)

    # Bulk set job parameters if required
    await _insert_parameters(param_updates, job_parameters_db, job_db)


async def add_heartbeat(
    data: dict[int, HeartbeatData],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    job_parameters_db: JobParametersDB,
) -> None:
    """Process and record heartbeat signals for multiple jobs.

    The function accepts heartbeat payloads keyed by job ID and ensures the
    system records liveness and any heartbeat-provided metadata. Behavior:
    - Looks up provided job IDs and validates they exist.
    - For jobs currently in ``MATCHED`` or ``STALLED`` schedules a status
      transition to ``RUNNING`` (with source set to ``"Heartbeat"``) by
      delegating to :func:`set_job_statuses`.
    - For jobs that do not require a status transition, updates their
      ``HeartBeatTime`` attribute via ``JobDB``.
    - Persists heartbeat SQL fields using ``job_db.add_heartbeat_data`` and
      stores any non-SQL fields in ``JobParametersDB``.

    Args:
        data (dict[int, HeartbeatData]): Mapping of job ID to heartbeat
            payloads.
        config (Config): Application configuration used for VO-specific
            behavior.
        job_db (JobDB): Database accessor for job records and attributes.
        job_logging_db (JobLoggingDB): Database accessor for job logging
            records.
        task_queue_db (TaskQueueDB): Database accessor for task-queue
            operations (used when status transitions affect queues).
        job_parameters_db (JobParametersDB): Accessor used to upsert
            non-SQL heartbeat parameters.

    Returns:
        None

    Raises:
        ValueError: If the provided job IDs cannot be resolved to existing
            jobs (lookup count differs from the input keys).
        Exceptions from underlying database accessors may propagate
            (e.g. connectivity or integrity errors).
    """
    # Find the current status of the jobs
    search_query: VectorSearchSpec = {
        "parameter": "JobID",
        "operator": VectorSearchOperator.IN,
        "values": list(data),
    }
    _, results = await job_db.search(
        parameters=["Status", "JobID"], search=[search_query], sorts=[]
    )
    if len(results) != len(data):
        raise ValueError(f"Failed to lookup job IDs: {data.keys()=} {results=}")
    status_changes = {
        int(result["JobID"]): {
            datetime.now(timezone.utc): JobStatusUpdate(
                Status=JobStatus.RUNNING,
                Source="Heartbeat",
            )
        }
        for result in results
        if result["Status"] in [JobStatus.MATCHED, JobStatus.STALLED]
    }

    async with TaskGroup() as tg:
        if status_changes:
            tg.create_task(
                set_job_statuses(
                    status_changes=status_changes,
                    config=config,
                    job_db=job_db,
                    job_logging_db=job_logging_db,
                    task_queue_db=task_queue_db,
                    job_parameters_db=job_parameters_db,
                )
            )

        if other_ids := set(data) - set(status_changes):
            # If there are no status changes, we still need to update the heartbeat time
            heartbeat_updates = {
                job_id: {"HeartBeatTime": utcnow()} for job_id in other_ids
            }
            tg.create_task(job_db.set_job_attributes(heartbeat_updates))

        os_data_by_job_id: defaultdict[int, dict[str, Any]] = defaultdict(dict)
        for job_id, job_data in data.items():
            sql_data = {}
            for key, value in job_data.model_dump(
                by_alias=True, exclude_defaults=True
            ).items():
                if key in job_db.heartbeat_fields:
                    sql_data[key] = value
                else:
                    os_data_by_job_id[job_id][key] = value

            if sql_data:
                tg.create_task(job_db.add_heartbeat_data(job_id, sql_data))

        await _insert_parameters(os_data_by_job_id, job_parameters_db, job_db)


async def _insert_parameters(
    updates: dict[int, dict[str, Any]],
    job_parameters_db: JobParametersDB,
    job_db: JobDB,
) -> None:
    """Upsert job parameters (if any) into the JobParametersDB.

    This helper accepts a mapping from job ID to parameter dictionaries and
    performs upserts into ``job_parameters_db``. Empty parameter mappings are
    ignored. The function first resolves the VO for each job by querying
    ``job_db.summary`` (required by the parameters index/template) and then
    issues concurrent upsert operations for each job's parameters.

    Args:
        updates (dict[int, dict[str, Any]]): Mapping of job ID to parameter
            key/value pairs to upsert. Jobs with empty parameter dicts are
            filtered out and ignored.
        job_parameters_db (JobParametersDB): Database accessor used to
            upsert parameters for a given VO and job ID.
        job_db (JobDB): Database accessor used to resolve the VO for each
            job ID present in ``updates``.

    Returns:
        None

    Raises:
        KeyError: If a job ID in ``updates`` is missing from the VO mapping
            returned by ``job_db.summary``.
        Exceptions from underlying DB accessors may propagate (e.g.
            connectivity or integrity errors).
    """
    updates = {job_id: params for job_id, params in updates.items() if params}
    if not updates:
        return
    # Get the VOs for the job IDs (required for the index template)
    job_vos = await job_db.summary(
        ["JobID", "VO"],
        [
            {
                "parameter": "JobID",
                "operator": VectorSearchOperator.IN,
                "values": list(updates),
            }
        ],
    )
    job_id_to_vo = {int(x["JobID"]): str(x["VO"]) for x in job_vos}
    # Upsert the parameters into the JobParametersDB
    # TODO: can we do a bulk upsert instead
    async with TaskGroup() as tg:
        for job_id, job_params in updates.items():
            tg.create_task(
                job_parameters_db.upsert(job_id_to_vo[job_id], job_id, job_params)
            )


async def get_job_commands(job_ids: Iterable[int], job_db: JobDB) -> list[JobCommand]:
    """Retrieve pending job commands for given job IDs and mark them as sent.

    The function fetches pending commands for the provided job IDs from the
    job database and marks those commands as ``Sent`` to avoid re-delivery.

    Args:
        job_ids (Iterable[int]): Iterable of job IDs to query commands for.
        job_db (JobDB): Database accessor to retrieve and update job commands.

    Returns:
        list[JobCommand]: List of pending ``JobCommand`` objects for the
            provided job IDs. If no commands exist an empty list is returned.

    Raises:
        Exceptions raised by ``job_db`` may propagate (e.g. connectivity
            errors).
    """
    return await job_db.get_job_commands(job_ids)
