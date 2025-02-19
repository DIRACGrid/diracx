from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timezone

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
    checkAndAddOwner,
    compressJDL,
    createJDLWithInitialStatus,
)
from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import (
    generateParametricJobs,
    getParameterVectorLength,
)
from pydantic import BaseModel

from diracx.core.models import (
    InsertedJob,
    JobLoggingRecord,
    JobStatus,
    UserInfo,
)
from diracx.db.sql.job.db import JobDB
from diracx.db.sql.job_logging.db import JobLoggingDB
from diracx.logic.jobs.utils import check_and_prepare_job

logger = logging.getLogger(__name__)


class JobSubmissionSpec(BaseModel):
    jdl: str
    owner: str
    owner_group: str
    initial_status: str
    initial_minor_status: str
    vo: str


MAX_PARAMETRIC_JOBS = 20


async def submit_jdl_jobs(
    job_definitions: list[str],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: UserInfo,
) -> list[InsertedJob]:
    """Submit a list of JDLs to the JobDB."""
    # TODO: that needs to go in the legacy adapter (Does it ? Because bulk submission is not supported there)
    for i in range(len(job_definitions)):
        job_definition = job_definitions[i].strip()
        if not (job_definition.startswith("[") and job_definition.endswith("]")):
            job_definition = f"[{job_definition}]"
        job_definitions[i] = job_definition

    if len(job_definitions) == 1:
        # Check if the job is a parametric one
        job_class_ad = ClassAd(job_definitions[0])
        result = getParameterVectorLength(job_class_ad)
        if not result["OK"]:
            # FIXME dont do this
            print("Issue with getParameterVectorLength", result["Message"])
            return result
        n_jobs = result["Value"]
        parametric_job = False
        if n_jobs is not None and n_jobs > 0:
            # if we are here, then jobDesc was the description of a parametric job. So we start unpacking
            parametric_job = True
            result = generateParametricJobs(job_class_ad)
            if not result["OK"]:
                # FIXME why?
                return result
            job_desc_list = result["Value"]
        else:
            # if we are here, then jobDesc was the description of a single job.
            job_desc_list = job_definitions
    else:
        # if we are here, then jobDesc is a list of JDLs
        # we need to check that none of them is a parametric
        for job_definition in job_definitions:
            res = getParameterVectorLength(ClassAd(job_definition))
            if not res["OK"]:
                raise ValueError(res["Message"])

            if res["Value"]:
                raise ValueError("You cannot submit parametric jobs in a bulk fashion")

        job_desc_list = job_definitions
        # parametric_job = True
        parametric_job = False

    # TODO: make the max number of jobs configurable in the CS
    if len(job_desc_list) > MAX_PARAMETRIC_JOBS:
        raise ValueError(
            f"Normal user cannot submit more than {MAX_PARAMETRIC_JOBS} jobs at once"
        )

    result = []

    if parametric_job:
        initial_status = JobStatus.SUBMITTING
        initial_minor_status = "Bulk transaction confirmation"
    else:
        initial_status = JobStatus.RECEIVED
        initial_minor_status = "Job accepted"

    try:
        submitted_job_ids = await create_jdl_jobs(
            [
                JobSubmissionSpec(
                    jdl=jdl,
                    owner=user_info.preferred_username,
                    owner_group=user_info.dirac_group,
                    initial_status=initial_status,
                    initial_minor_status=initial_minor_status,
                    vo=user_info.vo,
                )
                for jdl in job_desc_list
            ],
            job_db=job_db,
        )
    except ExceptionGroup as e:
        raise ValueError("JDL syntax error") from e

    logging.debug(
        f'Jobs added to the JobDB", "{submitted_job_ids} for {user_info.preferred_username}/{user_info.dirac_group}'
    )

    job_created_time = datetime.now(timezone.utc)
    await job_logging_db.insert_records(
        [
            JobLoggingRecord(
                job_id=int(job_id),
                status=initial_status,
                minor_status=initial_minor_status,
                application_status="Unknown",
                date=job_created_time,
                source="JobManager",
            )
            for job_id in submitted_job_ids
        ]
    )

    # if not parametric_job:
    #     self.__sendJobsToOptimizationMind(submitted_job_ids)

    return [
        InsertedJob(
            JobID=job_id,
            Status=initial_status,
            MinorStatus=initial_minor_status,
            TimeStamp=job_created_time,
        )
        for job_id in submitted_job_ids
    ]


async def create_jdl_jobs(jobs: list[JobSubmissionSpec], job_db: JobDB):
    """Create jobs from JDLs and insert them into the DB."""
    jobs_to_insert = {}
    jdls_to_update = {}
    inputdata_to_insert = {}
    original_jdls = []

    # generate the jobIDs first
    # TODO: should ForgivingTaskGroup be used?
    async with asyncio.TaskGroup() as tg:
        for job in jobs:
            original_jdl = deepcopy(job.jdl)
            job_manifest = returnValueOrRaise(
                checkAndAddOwner(original_jdl, job.owner, job.owner_group)
            )

            # Fix possible lack of brackets
            if original_jdl.strip()[0] != "[":
                original_jdl = f"[{original_jdl}]"

            original_jdls.append(
                (
                    original_jdl,
                    job_manifest,
                    tg.create_task(job_db.create_job(compressJDL(original_jdl))),
                )
            )

    async with asyncio.TaskGroup() as tg:
        for job, (original_jdl, job_manifest_, job_id_task) in zip(jobs, original_jdls):
            job_id = job_id_task.result()
            job_attrs = {
                "JobID": job_id,
                "LastUpdateTime": datetime.now(tz=timezone.utc),
                "SubmissionTime": datetime.now(tz=timezone.utc),
                "Owner": job.owner,
                "OwnerGroup": job.owner_group,
                "VO": job.vo,
            }

            job_manifest_.setOption("JobID", job_id)

            # 2.- Check JDL and Prepare DIRAC JDL
            job_jdl = job_manifest_.dumpAsJDL()

            # Replace the JobID placeholder if any
            if job_jdl.find("%j") != -1:
                job_jdl = job_jdl.replace("%j", str(job_id))

            class_ad_job = ClassAd(job_jdl)

            class_ad_req = ClassAd("[]")
            if not class_ad_job.isOK():
                # Rollback the entire transaction
                raise ValueError(f"Error in JDL syntax for job JDL: {original_jdl}")
            # TODO: check if that is actually true
            if class_ad_job.lookupAttribute("Parameters"):
                raise NotImplementedError("Parameters in the JDL are not supported")

            # TODO is this even needed?
            class_ad_job.insertAttributeInt("JobID", job_id)

            await check_and_prepare_job(
                job_id,
                class_ad_job,
                class_ad_req,
                job.owner,
                job.owner_group,
                job_attrs,
                job.vo,
                job_db,
            )
            job_jdl = createJDLWithInitialStatus(
                class_ad_job,
                class_ad_req,
                job_db.jdl_2_db_parameters,
                job_attrs,
                job.initial_status,
                job.initial_minor_status,
                modern=True,
            )

            jobs_to_insert[job_id] = job_attrs
            jdls_to_update[job_id] = compressJDL(job_jdl)

            if class_ad_job.lookupAttribute("InputData"):
                input_data = class_ad_job.getListFromExpression("InputData")
                inputdata_to_insert[job_id] = [lfn for lfn in input_data if lfn]

        tg.create_task(job_db.update_job_jdls(jdls_to_update))
        tg.create_task(job_db.insert_job_attributes(jobs_to_insert))

        if inputdata_to_insert:
            tg.create_task(job_db.insert_input_data(inputdata_to_insert))

    return list(jobs_to_insert.keys())
