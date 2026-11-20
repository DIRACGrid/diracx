"""Run statements over long lists of values in bounded ``IN (...)`` chunks."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TypeVar, TypeVarTuple, Unpack

from sqlalchemy import Row, Select
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.dml import UpdateBase

from diracx.core.utils import batched

T = TypeVar("T")
Ts = TypeVarTuple("Ts")

IN_CHUNK_SIZE = 999
"""Largest number of values put in one ``IN (...)`` list.

MySQL builds a range-optimiser structure for every value in an ``IN`` list
and falls back to a full table scan once it exceeds
``range_optimizer_max_mem_size``. MariaDB rewrites lists of
``in_predicate_conversion_threshold`` (default 1000) or more values into a
subquery over a temporary table, so the size stays just below that
default. Lists of this size avoid both.
"""


async def fetch_in_chunks(
    conn: AsyncConnection,
    build: Callable[[list[T]], Select[Unpack[Ts]]],
    values: Iterable[T],
    *,
    chunk_size: int = IN_CHUNK_SIZE,
) -> list[Row[Unpack[Ts]]]:
    """Run a query once per chunk of ``values`` and return every row.

    Duplicate values are dropped and the order of first appearance is kept.
    The result is only the same as one query over all the values when each
    row depends on a single value: no ``LIMIT``, ``OFFSET`` or aggregate
    across values, and ``ORDER BY`` and ``DISTINCT`` only apply within a
    chunk. ``FOR UPDATE`` locks are taken chunk by chunk, so sort ``values``
    first if the lock order matters.

    Args:
        conn: Connection to run the queries on.
        build: Returns the query for one chunk, filtering on ``IN (chunk)``.
        values: Values to filter on; nothing is run if it is empty.
        chunk_size: Largest number of values passed to ``build`` at once.

    Returns:
        The rows of every chunk, in chunk order.
    """
    rows: list[Row[Unpack[Ts]]] = []
    for chunk in batched(dict.fromkeys(values), chunk_size):
        rows.extend((await conn.execute(build(list(chunk)))).all())
    return rows


async def execute_in_chunks(
    conn: AsyncConnection,
    build: Callable[[list[T]], UpdateBase],
    values: Iterable[T],
    *,
    chunk_size: int = IN_CHUNK_SIZE,
) -> int:
    """Run an ``UPDATE`` or ``DELETE`` once per chunk of ``values``.

    Duplicate values are dropped and the order of first appearance is kept.

    Args:
        conn: Connection to run the statements on.
        build: Returns the statement for one chunk, filtering on
            ``IN (chunk)``.
        values: Values to filter on; nothing is run if it is empty.
        chunk_size: Largest number of values passed to ``build`` at once.

    Returns:
        The sum of the row counts of every chunk.
    """
    rowcount = 0
    for chunk in batched(dict.fromkeys(values), chunk_size):
        rowcount += (await conn.execute(build(list(chunk)))).rowcount
    return rowcount
