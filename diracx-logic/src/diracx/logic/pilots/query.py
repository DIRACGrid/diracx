from __future__ import annotations

from typing import Any

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models.search import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    SearchParams,
    SummaryParams,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql import PilotAgentsDB

MAX_PER_PAGE = 10000

# Pseudo-parameter accepted on POST /api/pilots/search. Resolves to a
# PilotID IN (...) filter via JobToPilotMapping.
JOB_ID_PSEUDO_PARAM = "JobID"
# Real column on PilotAgents that JobID would collide with if both
# were accepted in the same request body.
PILOT_ID_REAL_PARAM = "PilotID"


def _add_vo_constraint(
    body: SearchParams | SummaryParams, vo_constraint: str | None
) -> None:
    """Add a VO filter to the search body if a constraint is supplied.

    Admin callers pass `vo_constraint=None` to bypass the filter and query
    across all VOs. Mirrors the intra-VO pattern of `logic/jobs/query.py`.
    """
    if vo_constraint is None:
        return
    body.search.append(
        ScalarSearchSpec(
            parameter="VO",
            operator=ScalarSearchOperator.EQUAL,
            value=vo_constraint,
        )
    )


async def resolve_jobs_for_pilot_stamps(
    pilot_db: PilotAgentsDB, pilot_stamps: list[str]
) -> list[int]:
    """Resolve a batch of pilot stamps to the job IDs they have run.

    Used by `logic/jobs/query.py:search` to rewrite the `PilotStamp`
    pseudo-parameter into a concrete `JobID` vector filter.
    """
    return await pilot_db.job_ids_for_stamps(pilot_stamps)


async def _resolve_pilots_for_job_ids(
    pilot_db: PilotAgentsDB, job_ids: list[int]
) -> list[int]:
    """Resolve a batch of job IDs to the pilot IDs that have run them."""
    return await pilot_db.pilot_ids_for_job_ids(job_ids)


async def _rewrite_job_id_pseudo_param(
    pilot_db: PilotAgentsDB, body: SearchParams
) -> bool:
    """Rewrite any `JobID` pseudo-parameter in `body.search`.

    Collects every `JobID` filter, resolves them through
    `JobToPilotMapping`, removes the originals from `body.search`, and
    appends a single `PilotID IN (...)` vector filter. Returns `True`
    if the resolution produced an empty list (in which case the caller
    should short-circuit to an empty result), `False` otherwise.

    Supports `eq` and `in` operators only; every other operator raises
    `InvalidQueryError` because the join semantics are ambiguous.
    Combining a `JobID` pseudo-filter with a real `PilotID` filter in
    the same body is also refused.
    """
    matches = [
        spec for spec in body.search if spec.get("parameter") == JOB_ID_PSEUDO_PARAM
    ]
    if not matches:
        return False

    if any(spec.get("parameter") == PILOT_ID_REAL_PARAM for spec in body.search):
        raise InvalidQueryError(
            f"Cannot combine {JOB_ID_PSEUDO_PARAM!r} pseudo-parameter with a "
            f"real {PILOT_ID_REAL_PARAM!r} filter in the same request."
        )

    job_ids: list[int] = []
    for spec in matches:
        operator = spec.get("operator")
        if operator == ScalarSearchOperator.EQUAL:
            job_ids.append(int(spec["value"]))  # type: ignore[typeddict-item]
        elif operator == VectorSearchOperator.IN:
            job_ids.extend(int(v) for v in spec["values"])  # type: ignore[typeddict-item]
        else:
            raise InvalidQueryError(
                f"Operator {operator!r} is not supported on the "
                f"{JOB_ID_PSEUDO_PARAM!r} pseudo-parameter; use 'eq' or 'in'."
            )

    pilot_ids = await _resolve_pilots_for_job_ids(pilot_db, job_ids)
    body.search = [
        spec for spec in body.search if spec.get("parameter") != JOB_ID_PSEUDO_PARAM
    ]
    if not pilot_ids:
        return True
    body.search.append(
        VectorSearchSpec(
            parameter=PILOT_ID_REAL_PARAM,
            operator=VectorSearchOperator.IN,
            values=pilot_ids,
        )
    )
    return False


async def search(
    pilot_db: PilotAgentsDB,
    vo_constraint: str | None,
    page: int = 1,
    per_page: int = 100,
    body: SearchParams | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Retrieve information about pilots.

    `vo_constraint` restricts results to a single VO; pass `None` to
    query across VOs (reserved for service administrators).

    Accepts a `JobID` pseudo-parameter in `body.search` (`eq`/`in`
    only): it is resolved through `JobToPilotMapping` into a concrete
    `PilotID` vector filter before the main query runs. Mirrors the
    `PilotStamp` pseudo-parameter on `POST /api/jobs/search`.
    """
    if per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

    if body is None:
        body = SearchParams()

    empty_after_rewrite = await _rewrite_job_id_pseudo_param(pilot_db, body)
    if empty_after_rewrite:
        return 0, []

    _add_vo_constraint(body, vo_constraint)

    return await pilot_db.search_pilots(
        body.parameters,
        body.search,
        body.sort,
        distinct=body.distinct,
        page=page,
        per_page=per_page,
    )


async def summary(
    pilot_db: PilotAgentsDB,
    body: SummaryParams,
    vo_constraint: str | None,
):
    """Aggregate pilot counts suitable for plotting."""
    _add_vo_constraint(body, vo_constraint)
    return await pilot_db.pilot_summary(body.grouping, body.search)


async def get_pilots_by_stamp(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str],
    parameters: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return the pilots whose stamp is in `pilot_stamps`.

    Missing stamps are silently omitted from the result. Callers that care
    about completeness must compare the returned length to the input.
    `PilotStamp` is always included in the returned parameters so callers
    can identify which stamps were found.
    """
    if parameters is None:
        query_parameters: list[str] | None = None
    else:
        query_parameters = list(parameters)
        if "PilotStamp" not in query_parameters:
            query_parameters.append("PilotStamp")

    _, pilots = await pilot_db.search_pilots(
        parameters=query_parameters,
        search=[
            VectorSearchSpec(
                parameter="PilotStamp",
                operator=VectorSearchOperator.IN,
                values=pilot_stamps,
            )
        ],
        sorts=[],
        per_page=MAX_PER_PAGE,
    )
    return pilots
