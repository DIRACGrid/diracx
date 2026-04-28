from __future__ import annotations

import logging
from typing import Any

from diracx.core.config.schema import Config
from diracx.core.exceptions import InvalidQueryError
from diracx.core.models.search import (
    ScalarSearchOperator,
    SearchParams,
    SummaryParams,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.os.job_parameters import JobParametersDB
from diracx.db.sql.job.db import JobDB
from diracx.db.sql.job_logging.db import JobLoggingDB
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.logic.pilots.query import resolve_jobs_for_pilot_stamps

logger = logging.getLogger(__name__)


MAX_PER_PAGE = 10000

# Pseudo-parameter accepted on POST /api/jobs/search. Resolves to a
# JobID IN (...) filter via JobToPilotMapping.
PILOT_STAMP_PSEUDO_PARAM = "PilotStamp"
# Real Jobs column that PilotStamp would collide with if both were
# accepted in the same request body.
JOB_ID_REAL_PARAM = "JobID"


async def _rewrite_pilot_stamp_pseudo_param(
    pilot_db: PilotAgentsDB, body: SearchParams
) -> bool:
    """Rewrite any `PilotStamp` pseudo-parameter in `body.search`.

    Collects every `PilotStamp` filter, resolves them through
    `JobToPilotMapping`, removes the originals from `body.search`, and
    appends a single `JobID IN (...)` vector filter. Returns `True`
    if the resolution produced an empty list (the caller should
    short-circuit to an empty result), `False` otherwise.

    Supports `eq` and `in` operators only; every other operator raises
    `InvalidQueryError` because the join semantics are ambiguous.
    Combining a `PilotStamp` pseudo-filter with a real `JobID` filter
    in the same body is also refused.
    """
    matches = [
        spec
        for spec in body.search
        if spec.get("parameter") == PILOT_STAMP_PSEUDO_PARAM
    ]
    if not matches:
        return False

    if any(spec.get("parameter") == JOB_ID_REAL_PARAM for spec in body.search):
        raise InvalidQueryError(
            f"Cannot combine {PILOT_STAMP_PSEUDO_PARAM!r} pseudo-parameter "
            f"with a real {JOB_ID_REAL_PARAM!r} filter in the same request."
        )

    stamps: list[str] = []
    for spec in matches:
        operator = spec.get("operator")
        if operator == ScalarSearchOperator.EQUAL:
            stamps.append(str(spec["value"]))  # type: ignore[typeddict-item]
        elif operator == VectorSearchOperator.IN:
            stamps.extend(str(v) for v in spec["values"])  # type: ignore[typeddict-item]
        else:
            raise InvalidQueryError(
                f"Operator {operator!r} is not supported on the "
                f"{PILOT_STAMP_PSEUDO_PARAM!r} pseudo-parameter; "
                "use 'eq' or 'in'."
            )

    job_ids = await resolve_jobs_for_pilot_stamps(pilot_db, stamps)
    body.search = [
        spec
        for spec in body.search
        if spec.get("parameter") != PILOT_STAMP_PSEUDO_PARAM
    ]
    if not job_ids:
        return True
    body.search.append(
        VectorSearchSpec(
            parameter=JOB_ID_REAL_PARAM,
            operator=VectorSearchOperator.IN,
            values=job_ids,
        )
    )
    return False


async def search(
    config: Config,
    job_db: JobDB,
    job_parameters_db: JobParametersDB,
    job_logging_db: JobLoggingDB,
    pilot_db: PilotAgentsDB,
    preferred_username: str | None,
    vo: str,
    page: int = 1,
    per_page: int = 100,
    body: SearchParams | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Retrieve information about jobs.

    Accepts a `PilotStamp` pseudo-parameter in `body.search`
    (`eq`/`in` only): it is resolved through `JobToPilotMapping` into
    a concrete `JobID` vector filter before the main query runs. Mirrors
    the `JobID` pseudo-parameter on `POST /api/pilots/search`.
    """
    # Apply a limit to per_page to prevent abuse of the API
    if per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

    if body is None:
        body = SearchParams()

    empty_after_rewrite = await _rewrite_pilot_stamp_pseudo_param(pilot_db, body)
    if empty_after_rewrite:
        return 0, []

    if query_logging_info := ("LoggingInfo" in (body.parameters or [])):
        if body.parameters:
            body.parameters.remove("LoggingInfo")
            if not body.parameters:
                body.parameters = None
            else:
                body.parameters = ["JobID"] + (body.parameters or [])

    # TODO: Apply all the job policy stuff properly using user_info
    global_jobs_info = config.Operations[vo].Services.JobMonitoring.GlobalJobsInfo
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
    """Show information suitable for plotting."""
    global_jobs_info = config.Operations[vo].Services.JobMonitoring.GlobalJobsInfo
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
