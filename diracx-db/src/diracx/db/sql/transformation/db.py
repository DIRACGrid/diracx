"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from diracx.db.sql.utils import BaseSQLDB
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import Base as TransformationDBBase


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
        journalled. Rows that already exist are skipped, so concurrent or
        repeated runs are safe.

        Args:
            conn: Connection to the newly created database schema.
        """
        rows = [
            row
            for counter in journalled_counters(cls.metadata)
            for row in counter.lock_rows()
        ]
        if rows:
            await conn.execute(
                insert(counter_locks_table(cls.metadata))
                .prefix_with("IGNORE", dialect="mysql")
                .prefix_with("OR IGNORE", dialect="sqlite"),
                rows,
            )
