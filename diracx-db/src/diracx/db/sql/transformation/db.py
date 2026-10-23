"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from uuid_utils import UUID

from diracx.core.models.transformation import ParcelStatus, TransformationStatus
from diracx.db.sql.utils import BaseSQLDB
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import Base as TransformationDBBase
from .schema import (
    ComputeParcels,
    ComputeTransformations,
    Parcels,
    Processes,
    TransformationLog,
    Transformations,
)


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

    async def lock_transformation_process(
        self, transformation_id: UUID
    ) -> tuple[TransformationStatus, bytes] | None:
        """Lock a compute transformation's row and return what it runs.

        Holds the row until the transaction ends, so that its status and
        process cannot change underneath a caller about to change them.

        Args:
            transformation_id: The transformation.

        Returns:
            Its status and process identity, or ``None`` if there is no
            compute transformation with that id.
        """
        stmt = (
            select(Transformations.status, ComputeTransformations.process_id)
            .join(
                ComputeTransformations,
                ComputeTransformations.transformation_id
                == Transformations.transformation_id,
            )
            .where(Transformations.transformation_id == transformation_id)
            .with_for_update()
        )
        row = (await self.conn.execute(stmt)).one_or_none()
        if row is None:
            return None
        status, process_id = row
        return status, process_id

    async def set_transformation_process(
        self, transformation_id: UUID, process_id: bytes
    ) -> None:
        """Point a compute transformation at another stored process.

        Args:
            transformation_id: The transformation.
            process_id: Identity of the stored process it runs from now on.
        """
        await self.conn.execute(
            update(ComputeTransformations)
            .where(ComputeTransformations.transformation_id == transformation_id)
            .values(process_id=process_id)
        )

    async def repoint_unassigned_parcels(
        self, transformation_id: UUID, old: bytes, new: bytes
    ) -> int:
        """Point a transformation's not yet dispatched parcels at another process.

        Only ``Unassigned`` parcels running ``old`` change: from ``Reserved``
        on, a parcel is being or has been handed to a backend and stays a
        record of what it ran.

        Args:
            transformation_id: The transformation.
            old: Identity of the process being replaced.
            new: Identity of the process replacing it.

        Returns:
            How many parcels were repointed.
        """
        unassigned = select(Parcels.parcel_id).where(
            Parcels.transformation_id == transformation_id,
            Parcels.status == ParcelStatus.UNASSIGNED,
        )
        result = await self.conn.execute(
            update(ComputeParcels)
            .where(
                ComputeParcels.parcel_id.in_(unassigned),
                ComputeParcels.process_id == old,
            )
            .values(process_id=new)
        )
        return result.rowcount

    async def insert_transformation_log(
        self,
        transformation_id: UUID,
        event_type: str,
        author: str,
        *,
        message: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Append an event to a transformation's audit trail.

        Args:
            transformation_id: The transformation the event concerns.
            event_type: Kind of event.
            author: Who caused it.
            message: Free-text explanation.
            data: Event detail.
        """
        await self.conn.execute(
            insert(TransformationLog).values(
                transformation_id=transformation_id,
                event_type=event_type,
                author=author,
                message=message,
                data=data,
            )
        )


def _insert_ignore(table):
    """``INSERT`` that skips rows whose key already exists."""
    return (
        insert(table)
        .prefix_with("IGNORE", dialect="mysql")
        .prefix_with("OR IGNORE", dialect="sqlite")
    )
