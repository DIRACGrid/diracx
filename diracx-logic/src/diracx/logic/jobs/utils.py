"""Job submission utility helpers for DIRACX.

This module contains helper functions that adapt DIRACX configuration and
job submission state to DIRACCommon job preparation utilities. It also
handles recoverable submission failures by persisting partial job state
before propagating errors.
"""

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
    """Build the job manifest configuration dict for DIRACCommon helpers.

    This helper converts DIRACX's per-VO job description configuration into
    the structure expected by DIRACCommon manifest utilities (for example
    `checkAndAddOwner` and related functions).

    Args:
        config (Config): Global application configuration containing per-VO
            settings.
        vo (str): The VO identifier whose job description to use.

    Returns:
        dict: A mapping suitable for DIRACCommon manifest helpers with keys
            including ``defaultForGroup``, ``minForGroup``, ``maxForGroup``,
            ``allowedJobTypesForGroup`` and ``maxInputData``. Each of the
            ``*ForGroup`` entries contains ``CPUTime`` and ``Priority``
            entries derived from the VO job description.

    Raises:
        KeyError: If the provided ``vo`` is not present in
            ``config.operations``.
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
    """Build the configuration mapping for DIRACCommon's checkAndPrepareJob.

    Converts DIRACX per-VO configuration into the structure expected by
    DIRACCommon's ``checkAndPrepareJob`` helper. The returned mapping is
    passed as the ``config`` parameter to that function.

    Args:
        config (Config): Global application configuration containing per-VO
            operation settings.
        vo (str): VO identifier used to select the appropriate operation
            configuration.

    Returns:
        dict: Configuration mapping suitable for ``checkAndPrepareJob`` with
            keys: ``inputDataPolicyForVO``, ``softwareDistModuleForVO``,
            ``defaultCPUTimeForOwnerGroup``, and ``getDIRACPlatform``.

    Raises:
        KeyError: If the provided ``vo`` is not present in
            ``config.operations``.
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
    """Validate and prepare a submitted job's ClassAd and requirements.

    This wrapper invokes DIRACCommon's ``checkAndPrepareJob`` to validate the
    provided job ClassAd and to populate default attributes and requirements
    in ``class_ad_req``. If the DIRAC helper reports a recoverable
    submission error (matching ``EWMSSUBM``), the function will persist the
    provided ``job_attrs`` to the database via ``job_db.set_job_attributes``
    before propagating the error.

    Args:
        job_id (int): Identifier of the job being prepared.
        class_ad_job (ClassAd): Parsed ClassAd representing the job JDL.
        class_ad_req (ClassAd): ClassAd instance to populate job requirements.
        owner (str): Job owner username.
        owner_group (str): Job owner group.
        job_attrs (dict): Initial job attribute mapping (timestamps, owner,
            VO, etc.) used when persisting partial state on recoverable
            errors.
        vo (str): VO identifier used to select VO-specific behavior.
        job_db (JobDB): Database accessor used to persist attributes on
            recoverable failures.
        config (Config): Global configuration used to build the internal
            DIRACCommon configuration mapping.

    Returns:
        None

    Raises:
        SErrorException: Propagated when DIRACCommon's helper returns a
            non-OK result (re-raised via ``returnValueOrRaise``).
        KeyError: If the provided ``vo`` is missing from the application
            configuration when building the helper config.
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
