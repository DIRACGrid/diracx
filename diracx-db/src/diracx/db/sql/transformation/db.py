"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from diracx.db.sql.utils import BaseSQLDB
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import Base as TransformationDBBase
from .schema import Processes


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
            await conn.execute(_insert_ignore(counter_locks_table(cls.metadata)), rows)

    async def insert_processes(self, processes: Mapping[bytes, dict[str, Any]]) -> None:
        """Store process documents under their content hash.

        Storing a document that is already present is a no-op, so concurrent
        submissions of a shared process are safe.

        Args:
            processes: Documents keyed by content hash.
        """
        if processes:
            await self.conn.execute(
                _insert_ignore(Processes).values(
                    [{"process_id": k, "process": v} for k, v in processes.items()]
                )
            )

    async def get_processes(
        self, process_ids: Iterable[bytes]
    ) -> dict[bytes, dict[str, Any]]:
        """Fetch stored documents by content hash.

        Args:
            process_ids: Content hashes to fetch.

        Returns:
            The documents found, keyed by content hash.
        """
        stmt = select(Processes.process_id, Processes.process).where(
            Processes.process_id.in_(list(process_ids))
        )
        return {pid: body for pid, body in await self.conn.execute(stmt)}


def _insert_ignore(table):
    """``INSERT`` that skips rows whose key already exists."""
    return (
        insert(table)
        .prefix_with("IGNORE", dialect="mysql")
        .prefix_with("OR IGNORE", dialect="sqlite")
    )
