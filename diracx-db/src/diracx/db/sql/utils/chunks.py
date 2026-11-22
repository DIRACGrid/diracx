"""Run statements over long lists of values in bounded ``IN (...)`` chunks."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, TypeVar, TypeVarTuple, Unpack

import uuid_utils
from sqlalchemy import Row, Select
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.dml import UpdateBase

from diracx.core.utils import batched

if TYPE_CHECKING:
    from _typeshed import SupportsRichComparison

T = TypeVar("T", bound="SupportsRichComparison")
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


def _sort_key(value: Any) -> Any:
    """Order UUIDs by their bytes, which is also how ``BinaryUUID`` sorts.

    ``uuid.UUID`` and ``uuid_utils.UUID`` are both accepted as UUID values
    but cannot be compared with each other.
    """
    if isinstance(value, (uuid.UUID, uuid_utils.UUID)):
        return value.bytes
    return value


def _chunks(values: Iterable[T], chunk_size: int) -> Iterator[list[T]]:
    """Split the distinct ``values``, in sorted order, into chunks.

    Every chunk runs in the same transaction and keeps its row locks until
    the end, so two transactions taking chunks of overlapping values in
    different orders can each wait for a lock the other holds. Sorting first
    makes every caller take the locks in the same order, as long as the
    statement locks through an index on the ``IN`` column and Python orders
    the values as that index does (integers, bytes and UUIDs; strings only
    when the collation agrees with Python on case and trailing spaces).
    """
    for chunk in batched(sorted(set(values), key=_sort_key), chunk_size):
        yield list(chunk)


async def fetch_in_chunks(
    conn: AsyncConnection,
    build: Callable[[list[T]], Select[Unpack[Ts]]],
    values: Iterable[T],
    *,
    chunk_size: int = IN_CHUNK_SIZE,
) -> list[Row[Unpack[Ts]]]:
    """Run a query once per chunk of ``values`` and return every row.

    Duplicate values are dropped and the rest are sorted before being split,
    so ``FOR UPDATE`` locks are taken in ascending order. The result
    is only the same as one query over all the values when each row depends
    on a single value: no ``LIMIT``, ``OFFSET`` or aggregate across values,
    and ``ORDER BY`` and ``DISTINCT`` only apply within a chunk.

    Args:
        conn: Connection to run the queries on.
        build: Returns the query for one chunk, filtering on ``IN (chunk)``.
        values: Values to filter on; nothing is run if it is empty.
        chunk_size: Largest number of values passed to ``build`` at once.

    Returns:
        The rows of every chunk, in chunk order.
    """
    rows: list[Row[Unpack[Ts]]] = []
    for chunk in _chunks(values, chunk_size):
        rows.extend((await conn.execute(build(chunk))).all())
    return rows


async def execute_in_chunks(
    conn: AsyncConnection,
    build: Callable[[list[T]], UpdateBase],
    values: Iterable[T],
    *,
    chunk_size: int = IN_CHUNK_SIZE,
) -> int:
    """Run an ``UPDATE`` or ``DELETE`` once per chunk of ``values``.

    Duplicate values are dropped and the rest are sorted before being split,
    so row locks are taken in ascending order.

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
    for chunk in _chunks(values, chunk_size):
        rowcount += (await conn.execute(build(chunk))).rowcount
    return rowcount
