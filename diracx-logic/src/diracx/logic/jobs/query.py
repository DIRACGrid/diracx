from __future__ import annotations

import logging
from typing import Any

from diracx.core.config.schema import Config
from diracx.core.models import (
    JobSearchParams,
    JobSummaryParams,
    ScalarSearchOperator,
)
from diracx.db.os.job_parameters import JobParametersDB
from diracx.db.sql.job.db import JobDB
from diracx.db.sql.job_logging.db import JobLoggingDB

logger = logging.getLogger(__name__)


MAX_PER_PAGE = 10000


async def search(
    config: Config,
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
    job_logging_db: JobLoggingDB,
    preferred_username: str,
    page: int = 1,
    per_page: int = 100,
    body: JobSearchParams | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Retrieve information about jobs."""
    # Apply a limit to per_page to prevent abuse of the API
    if per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

    if body is None:
        body = JobSearchParams()

    if query_logging_info := ("LoggingInfo" in (body.parameters or [])):
        if body.parameters:
            body.parameters.remove("LoggingInfo")
            if not body.parameters:
                body.parameters = None
            else:
                body.parameters = ["JobID"] + (body.parameters or [])

    # TODO: Apply all the job policy stuff properly using user_info
    if not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo:
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                # TODO-385: https://github.com/DIRACGrid/diracx/issues/385
                # The value shoud be user_info.sub,
                # but since we historically rely on the preferred_username
                # we will keep using the preferred_username for now.
                "value": preferred_username,
            }
        )

    total, jobs = await job_db.search(
        body.parameters,
        body.search,
        body.sort,
        distinct=body.distinct,
        page=page,
        per_page=per_page,
    )

    if query_logging_info:
        job_logging_info = await job_logging_db.get_records(
            [job["JobID"] for job in jobs]
        )
        for job in jobs:
            job.update({"LoggingInfo": job_logging_info[job["JobID"]]})

    return total, jobs


async def summary(
    config: Config,
    job_db: JobDB,
    preferred_username: str,
    body: JobSummaryParams,
):
    """Show information suitable for plotting."""
    if not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo:
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                # TODO-385: https://github.com/DIRACGrid/diracx/issues/385
                # The value shoud be user_info.sub,
                # but since we historically rely on the preferred_username
                # we will keep using the preferred_username for now.
                "value": preferred_username,
            }
        )
    return await job_db.summary(body.grouping, body.search)
