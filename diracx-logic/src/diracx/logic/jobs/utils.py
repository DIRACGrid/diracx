from __future__ import annotations

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.DErrno import EWMSSUBM, cmpError
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
    checkAndPrepareJob,
)

from diracx.db.sql.job.db import JobDB


async def check_and_prepare_job(
    job_id: int,
    class_ad_job: ClassAd,
    class_ad_req: ClassAd,
    owner: str,
    owner_group: str,
    job_attrs: dict,
    vo: str,
    job_db: JobDB,
):
    """Check Consistency of Submitted JDL and set some defaults
    Prepare subJDL with Job Requirements.
    """
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
            await job_db.set_job_attributes({job_id: job_attrs})

        returnValueOrRaise(ret_val)
