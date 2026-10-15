"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from diracx.db.sql.utils import BaseSQLDB
from diracx.db.sql.utils.counters import counter_locks_table

from .schema import DATA_PARCELS_COUNTER, TRANSFORMATION_COUNTER
from .schema import Base as TransformationDBBase

COUNTERS = (TRANSFORMATION_COUNTER, DATA_PARCELS_COUNTER)


class TransformationDB(BaseSQLDB):
    """Workgraphs, transformations, their inputs and the parcels made of them.

    Attributes:
        metadata: SQLAlchemy metadata for the transformation tables.
    """

    metadata = TransformationDBBase.metadata

    @classmethod
    async def post_create(cls, conn: AsyncConnection) -> None:
        """Insert the ``CounterLocks`` rows of every counter that lacks them.

        The fold and rebuild operations of a journalled counter (DX-ADR-009)
        serialise on these rows, so they must exist before any delta is
        journalled.

        Args:
            conn: Connection to the newly created database schema.
        """
        locks = counter_locks_table(cls.metadata)
        existing = {
            (row.CounterName, row.Operation)
            for row in await conn.execute(select(locks))
        }
        missing = [
            row
            for counter in COUNTERS
            for row in counter.lock_rows()
            if (row["CounterName"], row["Operation"]) not in existing
        ]
        if missing:
            await conn.execute(insert(locks), missing)
