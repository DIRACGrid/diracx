"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

import uuid
from collections.abc import Collection, Iterable
from datetime import datetime
from functools import partial
from typing import Any, NamedTuple, TypeAlias

from sqlalchemy import Update, bindparam, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import (
    InputStatusConflictError,
    TransformationNotFoundError,
)
from diracx.core.hashing import hash_json
from diracx.core.models.search import SearchSpec, SortSpec
from diracx.core.models.transformation import (
    PARCEL_TRANSITIONS,
    FeederInput,
    InputStatus,
    PackerInput,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
)
from diracx.core.utils import batched
from diracx.db.sql.utils import (
    IN_CHUNK_SIZE,
    BaseSQLDB,
    execute_in_chunks,
    fetch_in_chunks,
    utcnow,
)
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import Base as TransformationDBBase
from .schema import (
    ComputeParcels,
    ComputeTransformations,
    Inputs,
    ParcelInputs,
    Parcels,
    RequirementsTemplates,
    TransformationLog,
    Transformations,
)

# BinaryUUID binds either kind; rows come back as uuid_utils.UUID
AnyUUID: TypeAlias = UUID | uuid.UUID


def _input_hash(lfn: str | None, descriptor: dict[str, Any] | None) -> bytes:
    """Return a 16-byte digest of the canonical ``(LFN, Descriptor)`` pair.

    Digests with :func:`~diracx.core.hashing.hash_json`, then keeps the
    leftmost 16 bytes so ``Inputs.InputHash`` stays ``BINARY(16)``.
    ``descriptor is None`` is treated as ``{}`` so missing and empty
    descriptors dedupe together. ``lfn is None`` stays JSON ``null`` (no file).

    Args:
        lfn: Logical file name, or ``None`` when the input is not a file.
        descriptor: Opaque JSON object, or ``None`` (normalised to ``{}``).

    Returns:
        Leftmost 16 bytes of the SHA-256 digest.
    """
    envelope = {"LFN": lfn, "Descriptor": {} if descriptor is None else descriptor}
    return hash_json(envelope)[:16]


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
            Its ``status``, ``kind``, ``feeder``, ``feeder_args``,
            ``feeder_state``, ``feeder_enabled``, ``packer`` and
            ``packer_args``.

        Raises:
            TransformationNotFoundError: If the transformation does not exist.
        """
        stmt = select(
            Transformations.status.label("status"),
            Transformations.kind.label("kind"),
            Transformations.feeder.label("feeder"),
            Transformations.feeder_args.label("feeder_args"),
            Transformations.feeder_state.label("feeder_state"),
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

    async def search_parcels(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Search for parcels in the database.

        Args:
            parameters: Optional parcel fields to include in each result.
            search: Search conditions to apply.
            sorts: Sort specifications to apply.
            distinct: Whether to return distinct results.
            per_page: Maximum number of results per page.
            page: Optional one-based page number for pagination.

        Returns:
            Total matching parcel count and the requested page of parcel
            records.
        """
        return await self._search(
            table=Parcels,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )

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
        # Locking in key order keeps concurrent writers from deadlocking, so
        # the IDs are sorted the way the column compares them before being
        # split into chunks.
        rows = await fetch_in_chunks(
            self.conn,
            lambda chunk: (
                select(Parcels.parcel_id)
                .where(Parcels.parcel_id.in_(chunk))
                .where(Parcels.status == from_status)
                .order_by(Parcels.parcel_id)
                .with_for_update()
            ),
            sorted(parcel_ids, key=lambda parcel_id: parcel_id.bytes),
        )
        moved = [row[0] for row in rows]
        if not moved:
            return []

        values: dict = {"status": to_status}
        if backend is not None:
            values["backend"] = backend
        if not PARCEL_TRANSITIONS[to_status]:
            # Reaching a final status (DX-ADR-005) stamps the completion time
            values["completed_at"] = utcnow()
        await execute_in_chunks(
            self.conn,
            lambda chunk: (
                update(Parcels)
                .where(Parcels.parcel_id.in_(chunk))
                .where(Parcels.status == from_status)
                .values(**values)
            ),
            moved,
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
        # Not split into chunks: the locks are meant to follow input order,
        # not the parcel order of the IN list, so sorting the parcel IDs
        # would not keep chunks in one global lock order. InnoDB locks rows
        # as it reads them, so the order also depends on the join order the
        # optimizer picks.
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

        def build(transformation_id: UUID, chunk: list[UUID]) -> Update:
            return (
                update(Inputs)
                .where(Inputs.transformation_id == transformation_id)
                .where(Inputs.input_id.in_(chunk))
                .where(Inputs.status == from_status)
                .values(**values)
            )

        for transformation_id, input_ids in by_transformation.items():
            await execute_in_chunks(
                self.conn, partial(build, transformation_id), input_ids
            )

        return len(locked)

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
        # Duplicates are kept: execute_in_chunks drops them, so a repeated
        # input makes the row count fall short and raises the conflict below
        input_ids = list(input_ids)
        if not input_ids:
            return
        rowcount = await execute_in_chunks(
            self.conn,
            lambda chunk: (
                update(Inputs)
                .where(
                    Inputs.transformation_id == transformation_id,
                    Inputs.input_id.in_(chunk),
                    Inputs.status == InputStatus.UNASSIGNED,
                )
                .values(**values)
            ),
            input_ids,
        )
        if rowcount != len(input_ids):
            raise InputStatusConflictError(
                transformation_id,
                f"{len(input_ids) - rowcount} of {len(input_ids)} inputs "
                "were not Unassigned",
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

    async def insert_unassigned_inputs(
        self,
        transformation_id: AnyUUID,
        inputs: Iterable[FeederInput],
        chunk_size: int = IN_CHUNK_SIZE,
    ) -> list[UUID]:
        """Insert ``Unassigned`` inputs that are not already present by hash.

        Deduplicates by ``InputHash`` (first proposal wins), including across
        chunks of this call. Hashes already stored for the transformation are
        skipped. Each chunk is at most ``chunk_size`` proposals: a ``SELECT``
        of existing hashes (split by ``fetch_in_chunks`` if ``chunk_size`` is
        above ``IN_CHUNK_SIZE``), then one ``INSERT`` of the new rows.
        A UUIDv7 is minted for each new row. Status is always ``Unassigned``.

        Args:
            transformation_id: Transformation that owns the inputs.
            inputs: Feeder proposals to insert, in priority order (consumed
                once, so a generator is accepted); empty input is a no-op.
            chunk_size: Maximum proposals read, and rows inserted, per
                ``INSERT``.

        Returns:
            ``input_id`` values of the rows that were inserted, in first-seen
            order.

        Raises:
            ValueError: If ``chunk_size`` is less than 1.
        """
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least 1")

        inserted: list[UUID] = []
        # Hashes already accepted in this call, so a later chunk does not
        # insert or re-select a duplicate of an earlier proposal.
        seen: set[bytes] = set()
        for chunk in batched(inputs, chunk_size):
            by_hash: dict[bytes, FeederInput] = {}
            for item in chunk:
                digest = _input_hash(item.lfn, item.descriptor)
                if digest in seen:
                    continue
                by_hash.setdefault(digest, item)
            if not by_hash:
                continue

            existing = {
                row[0]
                for row in await fetch_in_chunks(
                    self.conn,
                    lambda hashes: select(Inputs.input_hash).where(
                        Inputs.transformation_id == transformation_id,
                        Inputs.input_hash.in_(hashes),
                    ),
                    by_hash,
                )
            }
            seen.update(by_hash)
            rows = [
                {
                    "transformation_id": transformation_id,
                    "input_id": uuid7(),
                    "input_hash": input_hash,
                    "status": InputStatus.UNASSIGNED,
                    "lfn": item.lfn,
                    "lfn_size": item.lfn_size,
                    "descriptor": item.descriptor,
                    "delayed_until": item.delayed_until,
                }
                for input_hash, item in by_hash.items()
                if input_hash not in existing
            ]
            if not rows:
                continue
            await self.conn.execute(insert(Inputs).values(rows))
            # TODO(#1085): journal +len(rows) Unassigned
            inserted.extend(row["input_id"] for row in rows)
        return inserted

    async def set_feeder_state(
        self, transformation_id: AnyUUID, state: dict[str, Any] | None
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

    async def set_feeder_enabled(
        self, transformation_id: AnyUUID, enabled: bool
    ) -> None:
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
