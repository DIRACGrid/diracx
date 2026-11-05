"""Helpers for configuring, validating, and preparing submitted jobs."""

from __future__ import annotations

from functools import partial

from DIRACCommon.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRACCommon.Core.Utilities.DErrno import EWMSSUBM, cmpError
from DIRACCommon.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRACCommon.WorkloadManagementSystem.DB.JobDBUtils import checkAndPrepareJob

from diracx.core.config import Config
from diracx.core.resources import find_compatible_platforms
from diracx.db.sql.job.db import JobDB


def make_job_manifest_config(config: Config, vo: str):
    """Build DIRACCommon manifest settings from the VO configuration.

    Args:
        config: Installation configuration.
        vo: Virtual organization whose job-description settings to use.

    Returns:
        Configuration mapping consumed by DIRACCommon JDL utilities.
    """
    job_desc = config.operations[vo].job_description

    return {
        "defaultForGroup": {
            "CPUTime": job_desc.default_cpu_time,
            "Priority": job_desc.default_priority,
        },
        "minForGroup": {
            "CPUTime": job_desc.min_cpu_time,
            "Priority": job_desc.min_priority,
        },
        "maxForGroup": {
            "CPUTime": job_desc.max_cpu_time,
            "Priority": job_desc.max_priority,
        },
        "allowedJobTypesForGroup": job_desc.allowed_job_types,
        "maxInputData": job_desc.max_input_data,
    }


def make_check_and_prepare_job_config(config: Config, vo: str):
    """Build check-and-prepare settings from the VO configuration.

    Args:
        config: Installation configuration.
        vo: Virtual organization whose job settings to use.

    Returns:
        Configuration mapping consumed by DIRACCommon job preparation.
    """
    ops = config.operations[vo]
    return {
        "inputDataPolicyForVO": ops.input_data_policy.input_data_module,
        "softwareDistModuleForVO": ops.software_dist_module,
        "defaultCPUTimeForOwnerGroup": ops.job_description.default_cpu_time,
        "getDIRACPlatform": partial(find_compatible_platforms, config=config),
    }


async def check_and_prepare_job(
    job_id: int,
    class_ad_job: ClassAd,
    class_ad_req: ClassAd,
    owner: str,
    owner_group: str,
    job_attrs: dict,
    vo: str,
    job_db: JobDB,
    config: Config,
):
    """Check Consistency of Submitted JDL and set some defaults.

    Prepare subJDL with Job Requirements.

    Args:
        job_id: Identifier assigned to the job.
        class_ad_job: Parsed job JDL to validate and update.
        class_ad_req: ClassAd populated with computed job requirements.
        owner: Username submitting the job.
        owner_group: DIRAC group submitting the job.
        job_attrs: Job attributes to update during preparation.
        vo: Virtual organization that owns the job.
        job_db: Job database used for special submission-error updates.
        config: Installation configuration used to build preparation settings.

    Raises:
        SErrorException: If DIRACCommon rejects the job description.
    """
    # Create configuration dict for DIRACCommon function from diracx config
    dirac_config = make_check_and_prepare_job_config(config, vo)

    ret_val = checkAndPrepareJob(
        job_id,
        class_ad_job,
        class_ad_req,
        owner,
        owner_group,
        job_attrs,
        vo,
        config=dirac_config,
    )

    if not ret_val["OK"]:
        if cmpError(ret_val, EWMSSUBM):
            await job_db.set_job_attributes({job_id: job_attrs})

        returnValueOrRaise(ret_val)
