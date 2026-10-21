"""SQL database of the Transformation System (DX-ADR-004)."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
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
    InputStatus,
    PackerInput,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql.utils import BaseSQLDB
from diracx.db.sql.utils.counters import counter_locks_table, journalled_counters

from .schema import Base as TransformationDBBase
from .schema import (
    ComputeParcels,
    ComputeTransformations,
    Inputs,
    ParcelInputs,
    Parcels,
    Requirements,
    TransformationLog,
    Transformations,
)

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
            select(ComputeTransformations.process_id, Requirements.requirements)
            .outerjoin(
                Requirements,
                Requirements.requirements_id
                == ComputeTransformations.requirements_template_id,
            )
            .where(ComputeTransformations.transformation_id == transformation_id)
        )
        row = (await self.conn.execute(stmt)).one_or_none()
        if row is None:
            raise TransformationNotFoundError(transformation_id, "no compute facet")
        return row.ProcessID, row.Requirements or {}

    async def insert_requirements(
        self, requirements_id: bytes, requirements: dict[str, Any]
    ) -> None:
        """Store a requirement set unless one with its hash already exists.

        Args:
            requirements_id: Content hash of the canonicalised set.
            requirements: The requirement set.
        """
        await self.conn.execute(
            insert(Requirements)
            .prefix_with("IGNORE", dialect="mysql")
            .prefix_with("OR IGNORE", dialect="sqlite")
            .values(requirements_id=requirements_id, requirements=requirements)
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
