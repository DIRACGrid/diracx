"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Collection, Iterable
from datetime import datetime
from typing import Any, NamedTuple, TypeAlias

from sqlalchemy import bindparam, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from uuid_utils import UUID

from diracx.core.exceptions import (
    InputStatusConflictError,
    TransformationNotFoundError,
)
from diracx.core.models.transformation import (
    PARCEL_TRANSITIONS,
    CounterEntity,
    InputStatus,
    PackerInput,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql.utils import BaseSQLDB, utcnow
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import (
    TRANSFORMATION_COUNTER,
    ComputeParcels,
    ComputeTransformations,
    Inputs,
    ParcelInputs,
    Parcels,
    RequirementsTemplates,
    TransformationLog,
    Transformations,
)
from .schema import Base as TransformationDBBase

# BinaryUUID binds either kind; rows come back as uuid_utils.UUID
AnyUUID: TypeAlias = UUID | uuid.UUID


class NewComputeParcel(NamedTuple):
    """A compute parcel ready to be inserted.

    Attributes:
        parcel_id: UUIDv7 identifier minted for the parcel.
        requirements_id: Content hash of its stored requirements, if any.
        parameters: The CWL input object passed to the runner.
        annotations: Free-form annotations stored on the parcel.
        input_ids: Inputs the parcel is made of.
    """

    parcel_id: AnyUUID
    requirements_id: bytes | None
    parameters: dict[str, Any]
    annotations: dict[str, Any]
    input_ids: list[AnyUUID]


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

    async def get_transformation(self, transformation_id: AnyUUID) -> dict[str, Any]:
        """Get what the packer needs to know about a transformation.

        Args:
            transformation_id: The transformation.

        Returns:
            Its ``status``, ``kind``, ``feeder``, ``feeder_enabled``,
            ``packer`` and ``packer_args``.

        Raises:
            TransformationNotFoundError: If the transformation does not exist.
        """
        stmt = select(
            Transformations.status.label("status"),
            Transformations.kind.label("kind"),
            Transformations.feeder.label("feeder"),
            Transformations.feeder_enabled.label("feeder_enabled"),
            Transformations.packer.label("packer"),
            Transformations.packer_args.label("packer_args"),
        ).where(Transformations.transformation_id == transformation_id)
        row = (await self.conn.execute(stmt)).one_or_none()
        if row is None:
            raise TransformationNotFoundError(transformation_id)
        return dict(row._mapping)

    async def list_transformation_ids(self, status: TransformationStatus) -> list[UUID]:
        """List the transformations in a status.

        Args:
            status: Status to filter on.

        Returns:
            The identifiers of the matching transformations.
        """
        stmt = select(Transformations.transformation_id).where(
            Transformations.status == status
        )
        return list((await self.conn.scalars(stmt)).all())

    async def get_input_overview(
        self, transformation_id: AnyUUID
    ) -> dict[InputStatus, int]:
        """Count a transformation's inputs by status.

        Args:
            transformation_id: The transformation.

        Returns:
            The number of inputs in each status that has any.
        """
        # TODO(#1085): read the Transformation counter instead
        stmt = (
            select(Inputs.status, func.count())
            .where(Inputs.transformation_id == transformation_id)
            .group_by(Inputs.status)
        )
        return {
            InputStatus(status): count
            for status, count in (await self.conn.execute(stmt)).all()
        }

    async def claim_unassigned_inputs(
        self,
        transformation_id: AnyUUID,
        *,
        now: datetime,
        after_input_id: AnyUUID | None = None,
        limit: int,
    ) -> list[PackerInput]:
        """Lock a page of a transformation's packable inputs.

        Inputs are ``Unassigned`` ones whose ``DelayedUntil`` has passed, in
        ``InputID`` order. Rows locked by another transaction are skipped, and
        the locks are held until this transaction ends.

        Args:
            transformation_id: The transformation.
            now: Time against which ``DelayedUntil`` is compared.
            after_input_id: Last input of the previous page, if any.
            limit: Maximum number of inputs to claim.

        Returns:
            The claimed inputs.
        """
        stmt = (
            select(
                Inputs.input_id,
                Inputs.parent_input_id,
                Inputs.lfn,
                Inputs.lfn_size,
                Inputs.descriptor,
                Inputs.error_count,
            )
            .where(
                Inputs.transformation_id == transformation_id,
                Inputs.status == InputStatus.UNASSIGNED,
                Inputs.delayed_until.is_(None) | (Inputs.delayed_until <= now),
            )
            .order_by(Inputs.input_id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        if after_input_id is not None:
            stmt = stmt.where(Inputs.input_id > after_input_id)
        return [
            PackerInput(
                input_id=uuid.UUID(bytes=row.InputID.bytes),
                parent_input_id=(
                    uuid.UUID(bytes=row.ParentInputID.bytes)
                    if row.ParentInputID is not None
                    else None
                ),
                lfn=row.LFN,
                lfn_size=row.LFNSize,
                descriptor=row.Descriptor or {},
                error_count=row.ErrorCount,
            )
            for row in await self.conn.execute(stmt)
        ]

    async def get_compute_facet(
        self, transformation_id: AnyUUID
    ) -> tuple[bytes, dict[str, Any]]:
        """Get what a compute transformation's parcels run and require.

        Args:
            transformation_id: The transformation.

        Returns:
            The process hash and the requirements template, empty if the
            transformation has none.

        Raises:
            TransformationNotFoundError: If the transformation has no compute
                facet.
        """
        stmt = (
            select(
                ComputeTransformations.process_id,
                RequirementsTemplates.requirements_template,
            )
            .outerjoin(
                RequirementsTemplates,
                RequirementsTemplates.requirements_template_id
                == ComputeTransformations.requirements_template_id,
            )
            .where(ComputeTransformations.transformation_id == transformation_id)
        )
        row = (await self.conn.execute(stmt)).one_or_none()
        if row is None:
            raise TransformationNotFoundError(transformation_id, "no compute facet")
        return row.ProcessID, row.RequirementsTemplate or {}

    async def insert_requirements(
        self, requirements_id: bytes, requirements: dict[str, Any]
    ) -> None:
        """Store a requirement set unless one with its hash already exists.

        Args:
            requirements_id: Content hash of the canonicalised set.
            requirements: The requirement set.
        """
        await self.conn.execute(
            insert(RequirementsTemplates)
            .prefix_with("IGNORE", dialect="mysql")
            .prefix_with("OR IGNORE", dialect="sqlite")
            .values(
                requirements_template_id=requirements_id,
                requirements_template=requirements,
            )
        )

    async def insert_compute_parcels(
        self,
        transformation_id: AnyUUID,
        process_id: bytes,
        parcels: list[NewComputeParcel],
    ) -> None:
        """Insert compute parcels and assign their inputs to them.

        Args:
            transformation_id: Transformation the parcels belong to.
            process_id: Content hash of what the parcels run.
            parcels: The parcels to insert.

        Raises:
            InputStatusConflictError: If any of the inputs is not
                ``Unassigned``.
        """
        if not parcels:
            return
        await self.conn.execute(
            insert(Parcels),
            [
                {
                    "ParcelID": parcel.parcel_id,
                    "TransformationID": transformation_id,
                    "Kind": TransformationKind.COMPUTE,
                    "Annotations": parcel.annotations or None,
                }
                for parcel in parcels
            ],
        )
        await self.conn.execute(
            insert(ComputeParcels),
            [
                {
                    "ParcelID": parcel.parcel_id,
                    "ProcessID": process_id,
                    "RequirementsTemplateID": parcel.requirements_id,
                    "Parameters": parcel.parameters or None,
                }
                for parcel in parcels
            ],
        )
        links = [
            {
                "TransformationID": transformation_id,
                "ParcelID": parcel.parcel_id,
                "InputID": input_id,
            }
            for parcel in parcels
            for input_id in parcel.input_ids
        ]
        await self.conn.execute(insert(ParcelInputs), links)

        await self._update_unassigned_inputs(
            transformation_id,
            (link["InputID"] for link in links),
            status=InputStatus.ASSIGNED,
        )
        # Each input points at its own parcel, so this one is an executemany
        await self.conn.execute(
            update(Inputs)
            .where(
                Inputs.transformation_id == bindparam("b_transformation_id"),
                Inputs.input_id == bindparam("b_input_id"),
            )
            .values(last_parcel_id=bindparam("b_parcel_id")),
            [
                {
                    "b_transformation_id": transformation_id,
                    "b_input_id": link["InputID"],
                    "b_parcel_id": link["ParcelID"],
                }
                for link in links
            ],
        )
        # TODO(#1085): journal the inputs' Unassigned -> Assigned transition
        # and the new parcels as Unassigned

    async def set_inputs_problematic(
        self,
        transformation_id: AnyUUID,
        reasons: dict[AnyUUID, str],
        author: str,
    ) -> None:
        """Move ``Unassigned`` inputs to ``Problematic``, logging the reasons.

        ``Inputs`` has no column for the reason, so each one is recorded as an
        ``InputProblematic`` event in ``TransformationLog``.

        Args:
            transformation_id: Transformation the inputs belong to.
            reasons: Why each input is problematic.
            author: Component responsible for the change.

        Raises:
            InputStatusConflictError: If any of the inputs is not
                ``Unassigned``.
        """
        if not reasons:
            return
        await self._update_unassigned_inputs(
            transformation_id, reasons, status=InputStatus.PROBLEMATIC
        )
        await self.conn.execute(
            insert(TransformationLog),
            [
                {
                    "TransformationID": transformation_id,
                    "EventType": "InputProblematic",
                    "Author": author,
                    "Message": reason[:512],
                    "Data": {"InputID": str(input_id), "Reason": reason},
                }
                for input_id, reason in reasons.items()
            ],
        )
        # TODO(#1085): journal the inputs' Unassigned -> Problematic transition

    async def delay_inputs(
        self,
        transformation_id: AnyUUID,
        input_ids: Iterable[AnyUUID],
        until: datetime,
    ) -> None:
        """Withhold ``Unassigned`` inputs from the packer until a given time.

        Args:
            transformation_id: Transformation the inputs belong to.
            input_ids: The inputs to withhold.
            until: Time before which they are not claimed again.

        Raises:
            InputStatusConflictError: If any of the inputs is not
                ``Unassigned``.
        """
        await self._update_unassigned_inputs(
            transformation_id, input_ids, delayed_until=until
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
        """Move parcels from one status to another and journal the change.

        Only the parcels that are in ``from_status`` when the transaction
        locks them are moved; the others (unknown, or already moved by a
        concurrent writer) are silently skipped, and the caller can tell
        which by comparing with the returned list. This compare-and-set is
        what makes a claim safe without any lock outside the database.

        The counter deltas of DX-ADR-009 are derived from the rows locked here
        and appended in the caller's transaction, so they commit or roll back
        with the change. User parcels have no transformation and therefore no
        counter row.

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
        # Lock the rows first, so the deltas count exactly what is updated.
        # Locking in key order keeps concurrent writers from deadlocking.
        locked = (
            await self.conn.execute(
                select(Parcels.parcel_id, Parcels.transformation_id)
                .where(Parcels.parcel_id.in_(list(parcel_ids)))
                .where(Parcels.status == from_status)
                .order_by(Parcels.parcel_id)
                .with_for_update()
            )
        ).all()
        if not locked:
            return []
        moved = [parcel_id for parcel_id, _ in locked]

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

        await self._journal_transition(
            CounterEntity.PARCEL,
            Counter(
                transformation_id
                for _, transformation_id in locked
                if transformation_id is not None
            ),
            from_status,
            to_status,
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

        await self._journal_transition(
            CounterEntity.INPUT,
            Counter(
                {tid: len(input_ids) for tid, input_ids in by_transformation.items()}
            ),
            from_status,
            to_status,
        )
        return len(locked)

    async def _journal_transition(
        self,
        entity: CounterEntity,
        counts: Counter[UUID],
        from_status: ParcelStatus | InputStatus,
        to_status: ParcelStatus | InputStatus,
    ) -> None:
        """Append the counter deltas of a bulk transition (DX-ADR-009).

        A transition of ``N`` rows of one transformation journals two rows,
        ``(-N, from_status)`` and ``(+N, to_status)``, already aggregated.

        Args:
            entity: Whether inputs or parcels moved.
            counts: Number of rows moved, per transformation.
            from_status: Status the rows left.
            to_status: Status the rows entered.
        """
        rows = [
            {
                "TransformationID": transformation_id,
                "Entity": entity,
                "Status": status.value,
                "CountDelta": sign * count,
            }
            for transformation_id, count in counts.items()
            for status, sign in ((from_status, -1), (to_status, +1))
        ]
        if rows:
            await self.conn.execute(insert(TRANSFORMATION_COUNTER.journal), rows)

    async def _update_unassigned_inputs(
        self,
        transformation_id: AnyUUID,
        input_ids: Iterable[AnyUUID],
        **values: Any,
    ) -> None:
        """Update ``Unassigned`` inputs, failing if any is in another status.

        Args:
            transformation_id: Transformation the inputs belong to.
            input_ids: The inputs to update.
            **values: Columns to set.

        Raises:
            InputStatusConflictError: If any of the inputs is not
                ``Unassigned``.
        """
        input_ids = list(input_ids)
        if not input_ids:
            return
        result = await self.conn.execute(
            update(Inputs)
            .where(
                Inputs.transformation_id == transformation_id,
                Inputs.input_id.in_(input_ids),
                Inputs.status == InputStatus.UNASSIGNED,
            )
            .values(**values)
        )
        if result.rowcount != len(input_ids):
            raise InputStatusConflictError(
                transformation_id,
                f"{len(input_ids) - result.rowcount} of {len(input_ids)} inputs "
                "were not Unassigned",
            )
