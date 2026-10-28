"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from uuid_utils import UUID

from diracx.core.models.transformation import InputStatus, TransformationStatus
from diracx.db.sql.utils import BaseSQLDB
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import Base as TransformationDBBase
from .schema import Inputs, Transformations


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

    async def list_feedable_transformation_ids(self) -> list[UUID]:
        """Return IDs of transformations whose feeder may run.

        A transformation is feedable when it is ``Active``, ``FeederEnabled`` is
        true, and ``Feeder`` is set.
        """
        stmt = select(Transformations.transformation_id).where(
            Transformations.status == TransformationStatus.ACTIVE,
            Transformations.feeder_enabled.is_(True),
            Transformations.feeder.is_not(None),
        )
        result = await self.conn.execute(stmt)
        return list(result.scalars().all())

    async def get_feeder_columns(
        self, transformation_id: UUID
    ) -> dict[str, Any] | None:
        """Load feeder columns for one transformation.

        Args:
            transformation_id: Transformation whose feeder columns to load.

        Returns:
            Mapping with ``feeder``, ``feeder_args``, ``feeder_state``, and
            ``feeder_enabled``, or ``None`` if the transformation does not exist.
        """
        stmt = select(
            Transformations.feeder.label("feeder"),
            Transformations.feeder_args.label("feeder_args"),
            Transformations.feeder_state.label("feeder_state"),
            Transformations.feeder_enabled.label("feeder_enabled"),
        ).where(Transformations.transformation_id == transformation_id)
        row = (await self.conn.execute(stmt)).one_or_none()
        return dict(row._mapping) if row is not None else None

    async def insert_unassigned_inputs(
        self,
        transformation_id: UUID,
        inputs: Sequence[Mapping[str, Any]],
    ) -> None:
        """Insert ``Unassigned`` inputs, ignoring ``InputHash`` collisions.

        Each mapping must include ``input_id`` and ``input_hash``. Optional
        keys: ``lfn``, ``lfn_size``, ``descriptor``, ``delayed_until``. Status
        is always ``Unassigned``. Duplicate hashes within the transformation
        are skipped by the database (DX-ADR-004).

        Args:
            transformation_id: Transformation that owns the inputs.
            inputs: Rows to insert; empty sequences are a no-op.
        """
        if not inputs:
            return

        # Multiparam INSERT needs the same keys on every row.
        rows = [
            {
                "transformation_id": transformation_id,
                "input_id": item["input_id"],
                "input_hash": item["input_hash"],
                "status": InputStatus.UNASSIGNED,
                "lfn": item.get("lfn"),
                "lfn_size": item.get("lfn_size"),
                "descriptor": item.get("descriptor"),
                "delayed_until": item.get("delayed_until"),
            }
            for item in inputs
        ]

        await self.conn.execute(
            insert(Inputs)
            .prefix_with("IGNORE", dialect="mysql")
            .prefix_with("OR IGNORE", dialect="sqlite")
            .values(rows)
        )

    async def set_feeder_state(
        self, transformation_id: UUID, state: dict[str, Any] | None
    ) -> None:
        """Replace ``FeederState`` for a transformation.

        Args:
            transformation_id: Transformation whose bookmark to update.
            state: Opaque bookmark JSON, or ``None`` to clear the column.
        """
        await self.conn.execute(
            update(Transformations)
            .where(Transformations.transformation_id == transformation_id)
            .values(feeder_state=state)
        )

    async def set_feeder_enabled(self, transformation_id: UUID, enabled: bool) -> None:
        """Set whether the transformation's feeder may run.

        Args:
            transformation_id: Transformation to update.
            enabled: New ``FeederEnabled`` value.
        """
        await self.conn.execute(
            update(Transformations)
            .where(Transformations.transformation_id == transformation_id)
            .values(feeder_enabled=enabled)
        )
