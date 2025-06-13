from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Sequence, Type

from sqlalchemy import DateTime, RowMapping, asc, desc, func, select
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ColumnElement, expression

from diracx.core.exceptions import DiracFormattedError, InvalidQueryError

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine


def _get_columns(table, parameters):
    columns = [x for x in table.columns]
    if parameters:
        if unrecognised_parameters := set(parameters) - set(table.columns.keys()):
            raise InvalidQueryError(
                f"Unrecognised parameters requested {unrecognised_parameters}"
            )
        columns = [c for c in columns if c.name in parameters]
    return columns


class utcnow(expression.FunctionElement):  # noqa: N801
    type: TypeEngine = DateTime()
    inherit_cache: bool = True


@compiles(utcnow, "postgresql")
def pg_utcnow(element, compiler, **kw) -> str:
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(utcnow, "mssql")
def ms_utcnow(element, compiler, **kw) -> str:
    return "GETUTCDATE()"


@compiles(utcnow, "mysql")
def mysql_utcnow(element, compiler, **kw) -> str:
    return "(UTC_TIMESTAMP)"


@compiles(utcnow, "sqlite")
def sqlite_utcnow(element, compiler, **kw) -> str:
    return "DATETIME('now')"


class date_trunc(expression.FunctionElement):  # noqa: N801
    """Sqlalchemy function to truncate a date to a given resolution.

    Primarily used to be able to query for a specific resolution of a date e.g.

        select * from table where date_trunc('day', date_column) = '2021-01-01'
        select * from table where date_trunc('year', date_column) = '2021'
        select * from table where date_trunc('minute', date_column) = '2021-01-01 12:00'
    """

    type = DateTime()
    # Cache does not work as intended with time resolution values, so we disable it
    inherit_cache = False

    def __init__(self, *args, time_resolution, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._time_resolution = time_resolution


@compiles(date_trunc, "postgresql")
def pg_date_trunc(element, compiler, **kw):
    res = {
        "SECOND": "second",
        "MINUTE": "minute",
        "HOUR": "hour",
        "DAY": "day",
        "MONTH": "month",
        "YEAR": "year",
    }[element._time_resolution]
    return f"date_trunc('{res}', {compiler.process(element.clauses)})"


@compiles(date_trunc, "mysql")
def mysql_date_trunc(element, compiler, **kw):
    pattern = {
        "SECOND": "%Y-%m-%d %H:%i:%S",
        "MINUTE": "%Y-%m-%d %H:%i",
        "HOUR": "%Y-%m-%d %H",
        "DAY": "%Y-%m-%d",
        "MONTH": "%Y-%m",
        "YEAR": "%Y",
    }[element._time_resolution]

    (dt_col,) = list(element.clauses)
    return compiler.process(func.date_format(dt_col, pattern))


@compiles(date_trunc, "sqlite")
def sqlite_date_trunc(element, compiler, **kw):
    pattern = {
        "SECOND": "%Y-%m-%d %H:%M:%S",
        "MINUTE": "%Y-%m-%d %H:%M",
        "HOUR": "%Y-%m-%d %H",
        "DAY": "%Y-%m-%d",
        "MONTH": "%Y-%m",
        "YEAR": "%Y",
    }[element._time_resolution]
    (dt_col,) = list(element.clauses)
    return compiler.process(
        func.strftime(
            pattern,
            dt_col,
        )
    )


class days_since(expression.FunctionElement):  # noqa: N801
    """Sqlalchemy function to get the number of days since a given date.

    Primarily used to be able to query for a specific resolution of a date e.g.

        select * from table where days_since(date_column) = 0
        select * from table where days_since(date_column) = 1
    """

    type = DateTime()
    inherit_cache = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


@compiles(days_since, "postgresql")
def pg_days_since(element, compiler, **kw):
    return f"EXTRACT(DAY FROM (now() - {compiler.process(element.clauses)}))"


@compiles(days_since, "mysql")
def mysql_days_since(element, compiler, **kw):
    return f"DATEDIFF(NOW(), {compiler.process(element.clauses)})"


@compiles(days_since, "sqlite")
def sqlite_days_since(element, compiler, **kw):
    return f"julianday('now') - julianday({compiler.process(element.clauses)})"


def substract_date(**kwargs: float) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(**kwargs)


def hash(code: str):
    return hashlib.sha256(code.encode()).hexdigest()


def raw_hash(code: str):
    return hashlib.sha256(code.encode()).digest()


async def fetch_records_bulk_or_raises(
    conn: AsyncConnection,
    model: Any,  # Here, we currently must use `Any` because `declarative_base()` returns any
    missing_elements_error_cls: Type[DiracFormattedError],
    column_attribute_name: str,
    column_name: str,
    elements_to_fetch: list,
    order_by: tuple[str, str] | None = None,
    allow_more_than_one_result_per_input: bool = False,
    allow_no_result: bool = False,
) -> Sequence[RowMapping]:
    """Fetches a list of elements in a table, returns a list of elements.
    All elements from the `element_to_fetch` **must** be present.
    Raises the specified error if at least one is missing.

    Example:
    fetch_records_bulk_or_raises(
        self.conn,
        PilotAgents,
        PilotNotFound,
        "pilot_id",
        "PilotID",
        [1,2,3]
    )

    """
    assert elements_to_fetch

    # Get the column that needs to be in elements_to_fetch
    column = getattr(model, column_attribute_name)

    # Create the request
    stmt = select(model).with_for_update().where(column.in_(elements_to_fetch))

    if order_by:
        column_name_to_order_by, direction = order_by
        column_to_order_by = getattr(model, column_name_to_order_by)

        operator: ColumnElement = (
            asc(column_to_order_by) if direction == "asc" else desc(column_to_order_by)
        )

        stmt = stmt.order_by(operator)

    # Transform into dictionaries
    raw_results = await conn.execute(stmt)
    results = raw_results.mappings().all()

    # Detects duplicates
    if not allow_more_than_one_result_per_input:
        if len(results) > len(elements_to_fetch):
            raise RuntimeError("Seems to have duplicates in the database.")

    if not allow_no_result:
        # Checks if we have every elements we wanted
        found_keys = {row[column_name] for row in results}
        missing = set(elements_to_fetch) - found_keys

        if missing:
            raise missing_elements_error_cls(
                data={column_name: str(missing)}, detail=str(missing)
            )

    return results
