"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

from collections.abc import Collection

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from uuid_utils import UUID

from diracx.core.models.transformation import (
    PARCEL_TRANSITIONS,
    InputStatus,
    ParcelStatus,
    TransformationKind,
)
from diracx.db.sql.utils import BaseSQLDB, utcnow
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import Base as TransformationDBBase
from .schema import Inputs, ParcelInputs, Parcels


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

    async def get_parcel_ids(
        self,
        status: ParcelStatus,
        *,
        kind: TransformationKind | None = None,
        backends: Collection[str | None] | None = None,
        limit: int | None = None,
    ) -> list[UUID]:
        """Return the identifiers of the parcels in a status, in key order.

        The parcel identifier is a UUIDv7, so key order is creation order:
        the oldest parcels come first, which is the order the dispatcher
        claims in (DX-ADR-003).

        Args:
            status: Status the parcels must be in.
            kind: If given, only parcels of this kind.
            backends: If given, only parcels whose backend is one of these;
                ``None`` in the collection matches parcels with no backend
                yet, i.e. those the packer did not constrain (DX-ADR-003).
            limit: Maximum number of identifiers to return.

        Returns:
            The parcel identifiers, oldest first.
        """
        stmt = select(Parcels.parcel_id).where(Parcels.status == status)
        if kind is not None:
            stmt = stmt.where(Parcels.kind == kind)
        if backends is not None:
            names = [name for name in backends if name is not None]
            condition = Parcels.backend.in_(names)
            if None in backends:
                condition = condition | Parcels.backend.is_(None)
            stmt = stmt.where(condition)
        stmt = stmt.order_by(Parcels.parcel_id).limit(limit)
        return list((await self.conn.scalars(stmt)).all())

    async def set_parcel_status(
        self,
        parcel_ids: Collection[UUID],
        from_status: ParcelStatus,
        to_status: ParcelStatus,
        *,
        backend: str | None = None,
    ) -> list[UUID]:
        """Move parcels from one status to another.

        Only the parcels that are in ``from_status`` when the transaction
        locks them are moved; the others (unknown, or already moved by a
        concurrent writer) are silently skipped, and the caller can tell
        which by comparing with the returned list. This compare-and-set is
        what makes a claim safe without any lock outside the database.

        This method does not check that the transition is allowed: the state
        machine is enforced in ``diracx-logic`` (DX-ADR-005).

        Args:
            parcel_ids: Parcels to move.
            from_status: Status the parcels are expected to be in.
            to_status: Status to move them to.
            backend: If given, also record the backend the parcels go to.

        Returns:
            The identifiers of the parcels that were moved.
        """
        if not parcel_ids:
            return []
        # Lock the rows first, so the caller learns exactly what is updated.
        # Locking in key order keeps concurrent writers from deadlocking.
        moved = list(
            (
                await self.conn.scalars(
                    select(Parcels.parcel_id)
                    .where(Parcels.parcel_id.in_(list(parcel_ids)))
                    .where(Parcels.status == from_status)
                    .order_by(Parcels.parcel_id)
                    .with_for_update()
                )
            ).all()
        )
        if not moved:
            return []

        values: dict = {"status": to_status}
        if backend is not None:
            values["backend"] = backend
        if not PARCEL_TRANSITIONS[to_status]:
            # Reaching a final status (DX-ADR-005) stamps the completion time
            values["completed_at"] = utcnow()
        await self.conn.execute(
            update(Parcels)
            .where(Parcels.parcel_id.in_(moved))
            .where(Parcels.status == from_status)
            .values(**values)
        )
        return moved

    async def set_parcel_input_status(
        self,
        parcel_ids: Collection[UUID],
        from_status: InputStatus,
        to_status: InputStatus,
        *,
        increment_error_count: bool = False,
    ) -> int:
        """Move the inputs of some parcels from one status to another.

        This is how a parcel outcome reaches its inputs (DX-ADR-005). Only
        inputs currently in ``from_status`` are moved: ``ParcelInputs`` keeps
        the full history, so an input that has since moved on through another
        parcel is not touched.

        Args:
            parcel_ids: Parcels whose inputs are moved.
            from_status: Status the inputs are expected to be in.
            to_status: Status to move them to.
            increment_error_count: Whether to add one to each input's
                ``ErrorCount``, as a parcel failure does.

        Returns:
            The number of inputs moved.
        """
        if not parcel_ids:
            return 0
        locked = (
            await self.conn.execute(
                select(Inputs.transformation_id, Inputs.input_id)
                .join(
                    ParcelInputs,
                    (ParcelInputs.transformation_id == Inputs.transformation_id)
                    & (ParcelInputs.input_id == Inputs.input_id),
                )
                .where(ParcelInputs.parcel_id.in_(list(parcel_ids)))
                .where(Inputs.status == from_status)
                .order_by(Inputs.transformation_id, Inputs.input_id)
                .with_for_update()
            )
        ).all()
        if not locked:
            return 0

        # Group by transformation: the primary key leads with it, so each
        # update is a range on the clustered index
        by_transformation: dict[UUID, list[UUID]] = {}
        for transformation_id, input_id in locked:
            by_transformation.setdefault(transformation_id, []).append(input_id)

        values: dict = {"status": to_status}
        if increment_error_count:
            values["error_count"] = Inputs.error_count + 1
        for transformation_id, input_ids in by_transformation.items():
            await self.conn.execute(
                update(Inputs)
                .where(Inputs.transformation_id == transformation_id)
                .where(Inputs.input_id.in_(input_ids))
                .where(Inputs.status == from_status)
                .values(**values)
            )

        return len(locked)
