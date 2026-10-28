"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from uuid_utils import UUID

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models.search import SearchSpec, SortSpec
from diracx.core.models.transformation import (
    PARCEL_TRANSITIONS,
    InputStatus,
    ParcelStatus,
)
from diracx.db.sql.utils import (
    BaseSQLDB,
    _get_columns,
    apply_search_filters,
    apply_sort_constraints,
    utcnow,
)
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

    async def search_parcels(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        backends: Collection[str | None] | None = None,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Search for parcels in the database.

        This follows the ``search`` of the other databases, with one addition:
        a parcel's backend can be matched against a set of names that includes
        "no backend", which the search conditions cannot express.

        Args:
            parameters: Optional parcel fields to include in each result.
            search: Search conditions to apply.
            sorts: Sort specifications to apply.
            backends: If given, only parcels whose backend is one of these;
                ``None`` in the collection matches parcels with no backend
                yet, i.e. those the packer did not constrain (DX-ADR-003).
            distinct: Whether to return distinct results.
            per_page: Maximum number of results per page.
            page: Optional one-based page number for pagination.

        Returns:
            Total matching parcel count and the requested page of parcel
            records.
        """
        table = Parcels.__table__
        stmt = select(*_get_columns(table, parameters))
        stmt = apply_search_filters(table, stmt, search)
        if backends is not None:
            names = [name for name in backends if name is not None]
            condition = Parcels.backend.in_(names)
            if None in backends:
                condition = condition | Parcels.backend.is_(None)
            stmt = stmt.where(condition)
        stmt = apply_sort_constraints(table, stmt, sorts)
        if distinct:
            stmt = stmt.distinct()

        total = (
            await self.conn.execute(select(func.count()).select_from(stmt.alias()))
        ).scalar_one()

        if page is not None:
            if page < 1:
                raise InvalidQueryError("Page must be a positive integer")
            if per_page < 1:
                raise InvalidQueryError("Per page must be a positive integer")
            stmt = stmt.offset((page - 1) * per_page).limit(per_page)

        return total, [
            dict(row._mapping) async for row in (await self.conn.stream(stmt))
        ]

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
