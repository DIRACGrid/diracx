from __future__ import annotations

import logging
from typing import Any

from diracx.core.config.schema import Config
from diracx.core.models import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    SearchParams,
    SummaryParams,
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
    preferred_username: str | None,
    page: int = 1,
    per_page: int = 100,
    body: SearchParams | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Retrieve information about jobs."""
    # Apply a limit to per_page to prevent abuse of the API
    if per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

    if body is None:
        body = SearchParams()

    if query_logging_info := ("LoggingInfo" in (body.parameters or [])):
        if body.parameters:
            body.parameters.remove("LoggingInfo")
            if not body.parameters:
                body.parameters = None
            else:
                body.parameters = ["JobID"] + (body.parameters or [])

    # TODO: Apply all the job policy stuff properly using user_info
    if (
        not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo
        and preferred_username
    ):
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                # TODO-385: https://github.com/DIRACGrid/diracx/issues/385
                # The value should be user_info.sub,
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


async def get_input_data(job_db: JobDB, job_id: int) -> list[dict[str, Any]]:
    """Retrieve a job's input data."""
    _, input_data = await job_db.search_input_data(
        [],
        [
            ScalarSearchSpec(
                parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=job_id
            )
        ],
        [],
    )

    return input_data


async def get_job_parameters(
    job_parameters_db: JobParametersDB, job_id: int
) -> list[dict[str, Any]]:
    _, parameters = await job_parameters_db.search(
        [],
        [
            ScalarSearchSpec(
                parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=job_id
            )
        ],
        [],
    )

    return parameters


async def get_job_jdl(job_db: JobDB, job_id: int) -> dict[str, str | int]:
    _, jdls = await job_db.search_jdl(
        [],
        [
            ScalarSearchSpec(
                parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=job_id
            )
        ],
        sorts=[],
    )

    assert len(jdls) <= 1  # If not, there's a problem

    if len(jdls) == 1:
        return jdls[0]

    return {}


async def get_job_heartbeat_info(
    job_db: JobDB, job_id: int
) -> list[dict[str, str | int]]:
    _, jdls = await job_db.search_heartbeat_logging_info(
        [],
        [
            ScalarSearchSpec(
                parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=job_id
            )
        ],
        sorts=[],
    )

    return jdls


async def summary(
    config: Config,
    job_db: JobDB,
    preferred_username: str,
    body: SummaryParams,
):
    """Show information suitable for plotting."""
    if (
        not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo
        and preferred_username
    ):
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                # TODO-385: https://github.com/DIRACGrid/diracx/issues/385
                # The value should be user_info.sub,
                # but since we historically rely on the preferred_username
                # we will keep using the preferred_username for now.
                "value": preferred_username,
            }
        )
    return await job_db.summary(body.grouping, body.search)
