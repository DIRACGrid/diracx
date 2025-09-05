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
    """Create job manifest configuration for DIRACCommon functions from diracx config."""
    job_desc = config.Operations[vo].JobDescription

    return {
        "defaultForGroup": {
            "CPUTime": job_desc.DefaultCPUTime,
            "Priority": job_desc.DefaultPriority,
        },
        "minForGroup": {
            "CPUTime": job_desc.MinCPUTime,
            "Priority": job_desc.MinPriority,
        },
        "maxForGroup": {
            "CPUTime": job_desc.MaxCPUTime,
            "Priority": job_desc.MaxPriority,
        },
        "allowedJobTypesForGroup": job_desc.AllowedJobTypes,
        "maxInputData": job_desc.MaxInputData,
    }


def make_check_and_prepare_job_config(config: Config, vo: str):
    """Create checkAndPrepareJob configuration for DIRACCommon functions from diracx config."""
    ops = config.Operations[vo]
    return {
        "inputDataPolicyForVO": ops.InputDataPolicy.InputDataModule,
        "softwareDistModuleForVO": ops.ExternalsPolicy.SoftwareDistModule,
        "defaultCPUTimeForOwnerGroup": ops.JobDescription.DefaultCPUTime,
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
    """Check Consistency of Submitted JDL and set some defaults
    Prepare subJDL with Job Requirements.
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
