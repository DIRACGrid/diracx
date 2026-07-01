"""Job query helpers for DIRACX.

This module provides job search and summary helpers used by DIRACX logic
layer. It supports paginated searches, merged logging information, and
aggregated job summaries for dashboarding.
"""

from __future__ import annotations

import logging
from typing import Any

from diracx.core.config import Config
from diracx.core.models import (
    ScalarSearchOperator,
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
    vo: str,
    page: int = 1,
    per_page: int = 100,
    body: SearchParams | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Search for jobs with optional filters and pagination.

    Executes a job search using the provided ``SearchParams`` and returns a
    paginated list of matching job records together with the total match
    count. The ``per_page`` value is capped by ``MAX_PER_PAGE`` to prevent
    abusive requests. If ``body`` is omitted an empty ``SearchParams`` is
    used.

    The function also supports an internal ``LoggingInfo`` parameter: when
    requested it is removed from the underlying SQL query and logging
    information is fetched separately and merged into the returned job
    dictionaries under the ``LoggingInfo`` key.

    Args:
        config (Config): Application configuration used to determine VO
            specific settings (for example whether job listings are global).
        job_db (JobDB): Database accessor for job records and search.
        job_parameters_db (JobParametersDB): Accessor for job parameters.
        job_logging_db (JobLoggingDB): Accessor for job logging information.
        preferred_username (Optional[str]): Preferred username used to
            restrict results when VO does not expose global job listings.
        vo (str): Virtual organization identifier used to select VO
            specific configuration.
        page (int): 1-based page number to fetch.
        per_page (int): Number of items per page (will be limited to
            ``MAX_PER_PAGE``).
        body (Optional[SearchParams]): Search parameters including requested
            fields, filters, sorting and distinct flag.

    Returns:
        tuple[int, list[dict[str, Any]]]: A pair of ``(total, jobs)`` where
            ``total`` is the total number of matching records and ``jobs`` is
            the list of job records for the requested page. Each job is a
            dictionary mapping field names to values; if logging info was
            requested a ``LoggingInfo`` key will be present.

    Raises:
        Exceptions from the underlying database accessors may propagate
        (e.g. database connectivity errors).
    """
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
    global_jobs_info = config.operations[vo].services.job_monitoring.global_jobs_info
    if not global_jobs_info and preferred_username:
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


async def summary(
    config: Config,
    job_db: JobDB,
    preferred_username: str | None,
    vo: str,
    body: SummaryParams,
):
    """Compute aggregated job statistics suitable for plotting.

    Produces grouped summary counts for jobs matching the provided
    ``SummaryParams``. The result is intended for visualization or
    dashboard purposes. When job listings are not available for a ``vo``
    and a ``preferred_username`` is supplied, the search is
    restricted to jobs owned by that username.

    Args:
        config (Config): Application configuration used to determine VO
            specific behaviour (for example whether job listings are global).
        job_db (JobDB): Database accessor used to compute summaries.
        preferred_username (Optional[str]): Preferred username used to
            restrict results if VO does not expose global job listings.
        vo (str): Virtual organization identifier used to select VO
            specific configuration.
        body (SummaryParams): Parameters controlling grouping and filtering
            for the summary (e.g. grouping keys and search filters).

    Returns:
        Any: The result of ``job_db.summary(...)`` — typically a mapping
            or list representing grouped counts suitable for plotting.

    Raises:
        Exceptions from the underlying database accessor may propagate
        (e.g. database connectivity errors).
    """
    global_jobs_info = config.operations[vo].services.job_monitoring.global_jobs_info
    if not global_jobs_info and preferred_username:
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
