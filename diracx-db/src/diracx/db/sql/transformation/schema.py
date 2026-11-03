"""SQLAlchemy table schemas for the Transformation System (DX-ADR-004).

The tables fall into three groups:

* the definition side (``Workgraphs``, ``Ownership``, ``Transformations``,
  ``ComputeTransformations``, ``TransformationOutputs``, ``Actions`` and the
  content-addressed ``Processes``, ``ProcessReferences``, ``ProcessRegistry``,
  ``RequirementsTemplates`` and ``ResolvedHints``): few rows, written at submission;
* the execution side (``Inputs``, ``Parcels`` and their facets,
  ``ParcelInputs`` and ``ParcelOutputs``): bulk rows, written constantly;
* the accounting side (the journalled counters of DX-ADR-009 and the
  ``TransformationLog`` / ``WorkgraphLog`` audit trails).

Conventions:

* Minted identifiers are UUIDv7 stored as ``BINARY(16)``, so no table keyed by
  one carries a creation time: it is in the key.
* Content-addressed rows are keyed by a 32-byte hash of their canonical form.
* Status columns are native ``ENUM``s over the states of DX-ADR-005, whose
  values are appended to, never reordered. Columns whose values come from
  configuration or extensions (``Type``, ``Feeder``, ``Packer``, ``Backend``,
  ``EventType``) stay ``VARCHAR``.
* ``Inputs``, ``ParcelInputs`` and ``ParcelOutputs`` lead their primary key
  with the transformation (or the declared output), so that a
  transformation's rows are contiguous and cleaning is a range delete.

Invariants that span tables, such as "a ``ComputeParcels`` row exists exactly
when ``Parcels.Kind`` is ``Compute``", are enforced in ``diracx-logic``.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum as PyEnum
from typing import Annotated, Any

from sqlalchemy import (
    BINARY,
    JSON,
    BigInteger,
    CheckConstraint,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from uuid_utils import UUID

from diracx.core.models.transformation import (
    ActionResult,
    CounterEntity,
    InputStatus,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
    WorkgraphStatus,
)
from diracx.db.sql.utils import (
    BinaryUUID,
    str32,
    str64,
    str128,
    str512,
    utcnow,
)
from diracx.db.sql.utils.counters import BIGINT_AUTOINCREMENT, journalled_counter
from diracx.db.sql.utils.types import SmarterDateTime

# OIDC subjects, the identifier the DiracX authentication tables key on
str256 = Annotated[str, 256]
# XXH3-128 digest of an input's canonical (LFN, Descriptor)
bytes16 = Annotated[bytes, 16]
# Content hashes of canonicalised documents (DX-ADR-007)
bytes32 = Annotated[bytes, 32]

# LFNs are paths, so they compare case-sensitively
LFN_TYPE = String(512).with_variant(
    mysql.VARCHAR(512, collation="utf8mb4_bin"), "mysql"
)

created_ts = Annotated[
    datetime, mapped_column(SmarterDateTime(), server_default=utcnow())
]
updated_ts = Annotated[
    datetime,
    mapped_column(SmarterDateTime(), server_default=utcnow(), onupdate=utcnow()),
]


def native_enum(enum_type: type[PyEnum]) -> Enum:
    """Native ``ENUM`` column type storing the values of an enum, in order.

    Args:
        enum_type: Enum whose values are the allowed column values.

    Returns:
        The SQLAlchemy type.
    """
    return Enum(
        enum_type,
        native_enum=True,
        values_callable=lambda members: [member.value for member in members],
        name=enum_type.__name__.lower(),
    )


class Base(DeclarativeBase):
    """Declarative base with shared SQLAlchemy type mappings.

    Attributes:
        type_annotation_map: SQL types associated with shared Python aliases.
    """

    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str256: String(256),
        str512: String(512),
        UUID: BinaryUUID(),
        bytes16: BINARY(16),
        bytes32: BINARY(32),
        # Python None is SQL NULL, so "IS NULL" finds a cleared column
        dict[str, Any]: JSON(none_as_null=True),
    }


# --------------------------------------------------------------------------
# Definition side
# --------------------------------------------------------------------------


class Workgraphs(Base):
    """The deliverable users request: a DAG of transformations.

    Attributes:
        workgraph_id: UUIDv7 identifier of the workgraph.
        type: VO-defined label selecting default behaviour.
        status: Lifecycle status (DX-ADR-005).
        stripped_spec: Submitted CWL document with run bodies replaced by
            their ``Processes`` hashes; it declares the DAG's edges.
        annotations: Key-value annotations: human-facing labels and notes
            recorded by hooks and operators.
        hooks: Single-hook bindings (DX-ADR-006).
        updated_at: Time of the last change to the row.
    """

    __tablename__ = "Workgraphs"
    workgraph_id: Mapped[UUID] = mapped_column("WorkgraphID", primary_key=True)
    type: Mapped[str32] = mapped_column("Type")
    status: Mapped[WorkgraphStatus] = mapped_column(
        "Status",
        native_enum(WorkgraphStatus),
        default=WorkgraphStatus.NEW,
        server_default=WorkgraphStatus.NEW.value,
    )
    stripped_spec: Mapped[dict[str, Any]] = mapped_column("StrippedSpec")
    annotations: Mapped[dict[str, Any] | None] = mapped_column("Annotations")
    hooks: Mapped[dict[str, Any] | None] = mapped_column("Hooks")
    updated_at: Mapped[updated_ts] = mapped_column("UpdatedAt")

    __table_args__ = (Index("WorkgraphsStatus", "Status"),)


class Ownership(Base):
    """Who owns a workgraph and its transformations, or a standalone one.

    A workgraph and its member transformations reference the same row, so
    their owner cannot diverge.

    Attributes:
        ownership_id: UUIDv7 identifier of the ownership row.
        vo: Virtual organisation of the owner.
        sub: OIDC subject of the owner.
        workgraph_id: Owned workgraph, or ``None`` for a standalone
            transformation.
    """

    __tablename__ = "Ownership"
    ownership_id: Mapped[UUID] = mapped_column("OwnershipID", primary_key=True)
    vo: Mapped[str32] = mapped_column("VO")
    sub: Mapped[str256] = mapped_column("Sub")
    workgraph_id: Mapped[UUID | None] = mapped_column(
        "WorkgraphID", ForeignKey(Workgraphs.workgraph_id)
    )

    __table_args__ = (
        # At most one ownership row per workgraph
        UniqueConstraint("WorkgraphID", name="OwnershipWorkgraph"),
        Index("OwnershipVOSub", "VO", "Sub"),
    )


class Transformations(Base):
    """The definition every transformation has, whatever its kind.

    Attributes:
        transformation_id: UUIDv7 identifier of the transformation.
        ownership_id: Owner of the transformation.
        executing_identity: Identity the parcels run as.
        type: VO-defined label selecting default behaviour.
        kind: What the transformation's parcels become.
        status: Lifecycle status (DX-ADR-005).
        feeder: Input plugin, or ``None`` when inputs arrive by other means.
        feeder_args: Arguments of the feeder.
        feeder_state: The feeder's own bookmark, opaque to the core.
        feeder_enabled: Whether the feeder may run.
        packer: Input-packing plugin.
        packer_args: Arguments of the packer.
        annotations: Key-value annotations: human-facing labels and notes
            recorded by hooks and operators.
        hooks: Single-hook bindings (DX-ADR-006).
        updated_at: Time of the last change to the row.
    """

    __tablename__ = "Transformations"
    transformation_id: Mapped[UUID] = mapped_column(
        "TransformationID", primary_key=True
    )
    ownership_id: Mapped[UUID] = mapped_column(
        "OwnershipID", ForeignKey(Ownership.ownership_id)
    )
    executing_identity: Mapped[str64 | None] = mapped_column("ExecutingIdentity")
    type: Mapped[str32] = mapped_column("Type")
    kind: Mapped[TransformationKind] = mapped_column(
        "Kind", native_enum(TransformationKind)
    )
    status: Mapped[TransformationStatus] = mapped_column(
        "Status",
        native_enum(TransformationStatus),
        default=TransformationStatus.NEW,
        server_default=TransformationStatus.NEW.value,
    )
    feeder: Mapped[str64 | None] = mapped_column("Feeder")
    feeder_args: Mapped[dict[str, Any] | None] = mapped_column("FeederArgs")
    feeder_state: Mapped[dict[str, Any] | None] = mapped_column("FeederState")
    feeder_enabled: Mapped[bool] = mapped_column(
        "FeederEnabled", default=True, server_default="1"
    )
    packer: Mapped[str64] = mapped_column("Packer")
    packer_args: Mapped[dict[str, Any] | None] = mapped_column("PackerArgs")
    annotations: Mapped[dict[str, Any] | None] = mapped_column("Annotations")
    hooks: Mapped[dict[str, Any] | None] = mapped_column("Hooks")
    updated_at: Mapped[updated_ts] = mapped_column("UpdatedAt")

    __table_args__ = (
        Index("TransformationsOwnership", "OwnershipID"),
        Index("TransformationsStatus", "Status"),
        Index("TransformationsType", "Type"),
    )


class Processes(Base):
    """Content-addressed store of the CWL documents parcels run.

    Attributes:
        process_id: Content hash of the canonicalised document.
        process: The CWL document.
        created_at: When the document was first stored.
    """

    __tablename__ = "Processes"
    process_id: Mapped[bytes32] = mapped_column("ProcessID", primary_key=True)
    process: Mapped[dict[str, Any]] = mapped_column("Process")
    created_at: Mapped[created_ts] = mapped_column("CreatedAt")


class ProcessReferences(Base):
    """The references a stored process makes to other stored processes.

    Attributes:
        parent_process_id: Process whose body holds the reference.
        child_process_id: Process referenced.
    """

    __tablename__ = "ProcessReferences"
    parent_process_id: Mapped[bytes32] = mapped_column(
        "ParentProcessID", ForeignKey(Processes.process_id), primary_key=True
    )
    child_process_id: Mapped[bytes32] = mapped_column(
        "ChildProcessID", ForeignKey(Processes.process_id), primary_key=True
    )

    __table_args__ = (Index("ProcessReferencesChild", "ChildProcessID"),)


class ProcessRegistry(Base):
    """Names and versions for library processes, scoped per VO.

    A lookup over the content store, never an identity: submission resolves a
    name to a hash and stores the hash.

    Attributes:
        vo: Virtual organisation the name belongs to.
        name: Name of the process.
        version: Version of the process.
        process_id: Content hash the name and version resolve to.
        author: Who published the name.
        created_at: When the name was published.
    """

    __tablename__ = "ProcessRegistry"
    vo: Mapped[str32] = mapped_column("VO", primary_key=True)
    name: Mapped[str128] = mapped_column("Name", primary_key=True)
    version: Mapped[str64] = mapped_column("Version", primary_key=True)
    process_id: Mapped[bytes32] = mapped_column(
        "ProcessID", ForeignKey(Processes.process_id)
    )
    author: Mapped[str64] = mapped_column("Author")
    created_at: Mapped[created_ts] = mapped_column("CreatedAt")

    __table_args__ = (Index("ProcessRegistryProcess", "ProcessID"),)


class RequirementsTemplates(Base):
    """Content-addressed matching criteria, shared by parcels and templates.

    Attributes:
        requirements_template_id: Content hash of the canonicalised requirement set.
        requirements_template: The requirement set.
        created_at: When the set was first stored.
    """

    __tablename__ = "RequirementsTemplates"
    requirements_template_id: Mapped[bytes32] = mapped_column(
        "RequirementsTemplateID", primary_key=True
    )
    requirements_template: Mapped[dict[str, Any]] = mapped_column(
        "RequirementsTemplate"
    )
    created_at: Mapped[created_ts] = mapped_column("CreatedAt")


class ResolvedHints(Base):
    """Content-addressed hint values a parcel was dispatched under.

    Attributes:
        resolved_hints_id: Content hash of the canonicalised sidecar.
        hints: Effective hint values after configuration resolution.
        created_at: When the sidecar was first stored.
    """

    __tablename__ = "ResolvedHints"
    resolved_hints_id: Mapped[bytes32] = mapped_column(
        "ResolvedHintsID", primary_key=True
    )
    hints: Mapped[dict[str, Any]] = mapped_column("Hints")
    created_at: Mapped[created_ts] = mapped_column("CreatedAt")


class ComputeTransformations(Base):
    """The compute facet of a transformation.

    Attributes:
        transformation_id: The transformation this row extends.
        process_id: Content hash of what the parcels run; frozen once any
            parcel of the transformation exists.
        requirements_template_id: Base matching criteria the packer builds on.
    """

    __tablename__ = "ComputeTransformations"
    transformation_id: Mapped[UUID] = mapped_column(
        "TransformationID",
        ForeignKey(Transformations.transformation_id),
        primary_key=True,
    )
    process_id: Mapped[bytes32] = mapped_column(
        "ProcessID", ForeignKey(Processes.process_id)
    )
    requirements_template_id: Mapped[bytes32 | None] = mapped_column(
        "RequirementsTemplateID",
        ForeignKey(RequirementsTemplates.requirements_template_id),
    )


class TransformationOutputs(Base):
    """The declared outputs of each step of a compute transformation.

    Attributes:
        transformation_output_id: UUIDv7 identifier of the declared output.
        transformation_id: Transformation declaring the output.
        name: Step output id in the CWL document.
        intermediate: Whether the output is not a workgraph output, and may
            therefore be removed once its consumers are done.
    """

    __tablename__ = "TransformationOutputs"
    transformation_output_id: Mapped[UUID] = mapped_column(
        "TransformationOutputID", primary_key=True
    )
    transformation_id: Mapped[UUID] = mapped_column(
        "TransformationID", ForeignKey(Transformations.transformation_id)
    )
    name: Mapped[str64] = mapped_column("Name")
    intermediate: Mapped[bool] = mapped_column("Intermediate")

    __table_args__ = (
        UniqueConstraint("TransformationID", "Name", name="TransformationOutputName"),
    )


class Actions(Base):
    """Ordered lifecycle actions of a workgraph or transformation.

    Attributes:
        action_id: Database-generated identifier.
        workgraph_id: Owning workgraph, if the owner is a workgraph.
        transformation_id: Owning transformation, if the owner is one.
        state: State whose list the action belongs to.
        position: Run order within that list.
        action: Name of the action.
        args: Arguments of the action.
        result: Outcome of the last run, ``None`` until run.
        message: Message recorded with the result.
        author: Operator who forced or reset the result.
        updated_at: Time of the last result.
    """

    __tablename__ = "Actions"
    action_id: Mapped[int] = mapped_column(
        "ActionID", BIGINT_AUTOINCREMENT, primary_key=True, autoincrement=True
    )
    workgraph_id: Mapped[UUID | None] = mapped_column(
        "WorkgraphID", ForeignKey(Workgraphs.workgraph_id)
    )
    transformation_id: Mapped[UUID | None] = mapped_column(
        "TransformationID", ForeignKey(Transformations.transformation_id)
    )
    state: Mapped[str32] = mapped_column("State")
    position: Mapped[int] = mapped_column("Position")
    action: Mapped[str64] = mapped_column("Action")
    args: Mapped[dict[str, Any] | None] = mapped_column("Args")
    result: Mapped[ActionResult | None] = mapped_column(
        "Result", native_enum(ActionResult)
    )
    message: Mapped[str512 | None] = mapped_column("Message")
    author: Mapped[str64 | None] = mapped_column("Author")
    updated_at: Mapped[datetime | None] = mapped_column("UpdatedAt", SmarterDateTime())

    __table_args__ = (
        CheckConstraint(
            "(WorkgraphID IS NULL) <> (TransformationID IS NULL)",
            name="ActionsOneOwner",
        ),
        UniqueConstraint(
            "WorkgraphID", "State", "Position", name="ActionsWorkgraphPosition"
        ),
        UniqueConstraint(
            "TransformationID",
            "State",
            "Position",
            name="ActionsTransformationPosition",
        ),
    )


# --------------------------------------------------------------------------
# Execution side
# --------------------------------------------------------------------------


class Inputs(Base):
    """One unit of processable input of a transformation.

    Usually a file, possibly a fraction of one (a mask in ``Descriptor``),
    possibly not a file at all (a seed, an event count).

    Attributes:
        transformation_id: Transformation the input belongs to.
        input_id: UUIDv7 identifier of the input.
        parent_input_id: Input this one was split from, in the same
            transformation.
        input_hash: XXH3-128 digest of the canonical ``(LFN, Descriptor)``,
            unique within the transformation; what inputs are deduplicated on.
        lfn: Logical file name, promoted out of the descriptor to be indexed.
        lfn_size: Size of the whole file, whatever the mask selects.
        descriptor: Everything else describing the input, opaque to the core.
        status: Lifecycle status (DX-ADR-005).
        error_count: Failed parcels the input was part of since the last reset.
        delayed_until: Time before which the packer must not claim the input.
        last_parcel_id: Most recent parcel the input was part of.
        updated_at: Time of the last change to the row.
    """

    __tablename__ = "Inputs"
    transformation_id: Mapped[UUID] = mapped_column(
        "TransformationID",
        ForeignKey(Transformations.transformation_id),
        primary_key=True,
    )
    input_id: Mapped[UUID] = mapped_column("InputID", primary_key=True)
    parent_input_id: Mapped[UUID | None] = mapped_column("ParentInputID")
    input_hash: Mapped[bytes16] = mapped_column("InputHash")
    lfn: Mapped[str | None] = mapped_column("LFN", LFN_TYPE)
    lfn_size: Mapped[int | None] = mapped_column("LFNSize", BigInteger)
    descriptor: Mapped[dict[str, Any] | None] = mapped_column("Descriptor")
    status: Mapped[InputStatus] = mapped_column(
        "Status",
        native_enum(InputStatus),
        default=InputStatus.UNASSIGNED,
        server_default=InputStatus.UNASSIGNED.value,
    )
    error_count: Mapped[int] = mapped_column(
        "ErrorCount", default=0, server_default="0"
    )
    delayed_until: Mapped[datetime | None] = mapped_column(
        "DelayedUntil", SmarterDateTime()
    )
    last_parcel_id: Mapped[UUID | None] = mapped_column("LastParcelID")
    updated_at: Mapped[updated_ts] = mapped_column("UpdatedAt")

    __table_args__ = (
        # A split child is always in the same transformation as its parent,
        # which is what lets the reference be composite
        ForeignKeyConstraint(
            ["TransformationID", "ParentInputID"],
            ["Inputs.TransformationID", "Inputs.InputID"],
            name="InputsParent",
            # A range delete over the transformation must not be refused
            # because a parent row goes before its children. InnoDB stops
            # cascading after 15 levels, and split depth is not bounded, so
            # cleaning deletes split lineages bottom-up rather than relying
            # on the cascade alone.
            ondelete="CASCADE",
        ),
        UniqueConstraint("TransformationID", "InputHash", name="InputsHash"),
        Index("InputsStatus", "TransformationID", "Status"),
        Index("InputsLFN", "TransformationID", "LFN"),
        Index("InputsParentInput", "TransformationID", "ParentInputID"),
    )


class Parcels(Base):
    """Dispatch bookkeeping of a unit of work, user job or packed inputs.

    A transformation parcel has a ``TransformationID``; a user parcel has a
    ``UserParcels`` row instead. Every column the sweeps touch is here, so
    none of them joins.

    Attributes:
        parcel_id: UUIDv7 identifier of the parcel.
        transformation_id: Transformation that created the parcel, or ``None``
            for a user parcel.
        kind: Denormalised from the transformation; user parcels are always
            ``Compute``.
        status: Lifecycle status (DX-ADR-005).
        backend: Backend chosen by the dispatcher (DX-ADR-003).
        annotations: Key-value annotations: output manifest and backend
            extras.
        updated_at: Time of the last change to the row.
        completed_at: Time the parcel reached a final status.
    """

    __tablename__ = "Parcels"
    parcel_id: Mapped[UUID] = mapped_column("ParcelID", primary_key=True)
    transformation_id: Mapped[UUID | None] = mapped_column(
        "TransformationID", ForeignKey(Transformations.transformation_id)
    )
    kind: Mapped[TransformationKind] = mapped_column(
        "Kind", native_enum(TransformationKind)
    )
    status: Mapped[ParcelStatus] = mapped_column(
        "Status",
        native_enum(ParcelStatus),
        default=ParcelStatus.UNASSIGNED,
        server_default=ParcelStatus.UNASSIGNED.value,
    )
    backend: Mapped[str32 | None] = mapped_column("Backend")
    annotations: Mapped[dict[str, Any] | None] = mapped_column("Annotations")
    updated_at: Mapped[updated_ts] = mapped_column("UpdatedAt")
    completed_at: Mapped[datetime | None] = mapped_column(
        "CompletedAt", SmarterDateTime()
    )

    __table_args__ = (
        Index("ParcelsTransformationStatus", "TransformationID", "Status"),
        Index("ParcelsBackendStatus", "Backend", "Status"),
        Index("ParcelsKindStatus", "Kind", "Status"),
    )


class UserParcels(Base):
    """The submitter of a user parcel.

    Attributes:
        parcel_id: The parcel this row extends.
        vo: Virtual organisation of the submitter.
        sub: OIDC subject of the submitter.
    """

    __tablename__ = "UserParcels"
    parcel_id: Mapped[UUID] = mapped_column(
        "ParcelID", ForeignKey(Parcels.parcel_id), primary_key=True
    )
    vo: Mapped[str32] = mapped_column("VO")
    sub: Mapped[str256] = mapped_column("Sub")

    __table_args__ = (Index("UserParcelsVOSub", "VO", "Sub"),)


class ComputeParcels(Base):
    """The compute facet of a parcel: what it runs and where it may run.

    Attributes:
        parcel_id: The parcel this row extends.
        process_id: Content hash of the process run.
        requirements_template_id: Deduplicated matching criteria.
        resolved_hints_id: Configuration defaults in force at dispatch.
        parameters: CWL input object for this run.
    """

    __tablename__ = "ComputeParcels"
    parcel_id: Mapped[UUID] = mapped_column(
        "ParcelID", ForeignKey(Parcels.parcel_id), primary_key=True
    )
    process_id: Mapped[bytes32] = mapped_column(
        "ProcessID", ForeignKey(Processes.process_id)
    )
    requirements_template_id: Mapped[bytes32 | None] = mapped_column(
        "RequirementsTemplateID",
        ForeignKey(RequirementsTemplates.requirements_template_id),
    )
    resolved_hints_id: Mapped[bytes32 | None] = mapped_column(
        "ResolvedHintsID", ForeignKey(ResolvedHints.resolved_hints_id)
    )
    parameters: Mapped[dict[str, Any] | None] = mapped_column("Parameters")


class DataParcels(Base):
    """The data facet of a parcel: the request the packer decided on.

    Attributes:
        parcel_id: The parcel this row extends.
        request: DIRAC RMS request body (DX-ADR-008).
    """

    __tablename__ = "DataParcels"
    parcel_id: Mapped[UUID] = mapped_column(
        "ParcelID", ForeignKey(Parcels.parcel_id), primary_key=True
    )
    request: Mapped[dict[str, Any]] = mapped_column("Request")


class DataParcelDeltas(Base):
    """What a data parcel's request costs one storage element.

    Attributes:
        parcel_id: The data parcel.
        storage_element: Storage element affected.
        files_added: Files written there; never negative.
        bytes_added: Bytes written there; never negative.
        files_freed: Files freed there; never negative.
        bytes_freed: Bytes freed there; never negative.
    """

    __tablename__ = "DataParcelDeltas"
    parcel_id: Mapped[UUID] = mapped_column(
        "ParcelID", ForeignKey(DataParcels.parcel_id), primary_key=True
    )
    storage_element: Mapped[str64] = mapped_column("StorageElement", primary_key=True)
    files_added: Mapped[int] = mapped_column("FilesAdded", BigInteger)
    bytes_added: Mapped[int] = mapped_column("BytesAdded", BigInteger)
    files_freed: Mapped[int] = mapped_column("FilesFreed", BigInteger)
    bytes_freed: Mapped[int] = mapped_column("BytesFreed", BigInteger)

    __table_args__ = (
        CheckConstraint(
            "FilesAdded >= 0 AND BytesAdded >= 0 "
            "AND FilesFreed >= 0 AND BytesFreed >= 0",
            name="DataParcelDeltasNonNegative",
        ),
        Index("DataParcelDeltasSE", "StorageElement"),
    )


class ParcelInputs(Base):
    """Which inputs each transformation parcel was made of.

    Rows are never deleted on failure, so this is the complete processing
    history of every input.

    Attributes:
        transformation_id: Transformation of the parcel and the input.
        parcel_id: The parcel.
        input_id: One of its inputs.
    """

    __tablename__ = "ParcelInputs"
    transformation_id: Mapped[UUID] = mapped_column(
        "TransformationID", primary_key=True
    )
    parcel_id: Mapped[UUID] = mapped_column(
        "ParcelID", ForeignKey(Parcels.parcel_id), primary_key=True
    )
    input_id: Mapped[UUID] = mapped_column("InputID", primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["TransformationID", "InputID"],
            ["Inputs.TransformationID", "Inputs.InputID"],
            name="ParcelInputsInput",
        ),
        Index("ParcelInputsInputID", "TransformationID", "InputID"),
    )


class ParcelOutputs(Base):
    """The files compute parcels produced, under their declared output.

    Attributes:
        transformation_output_id: Declared output the file belongs to.
        output_id: UUIDv7 identifier; its order is the feed order.
        parcel_id: Parcel that produced the file.
        lfn: Logical file name of the file.
        lfn_size: Size of the file.
    """

    __tablename__ = "ParcelOutputs"
    transformation_output_id: Mapped[UUID] = mapped_column(
        "TransformationOutputID",
        ForeignKey(TransformationOutputs.transformation_output_id),
        primary_key=True,
    )
    output_id: Mapped[UUID] = mapped_column("OutputID", primary_key=True)
    parcel_id: Mapped[UUID] = mapped_column("ParcelID", ForeignKey(Parcels.parcel_id))
    lfn: Mapped[str] = mapped_column("LFN", LFN_TYPE)
    lfn_size: Mapped[int | None] = mapped_column("LFNSize", BigInteger)

    __table_args__ = (
        Index("ParcelOutputsParcel", "ParcelID"),
        Index("ParcelOutputsLFN", "TransformationOutputID", "LFN"),
    )


# --------------------------------------------------------------------------
# Accounting side
# --------------------------------------------------------------------------

# Inputs and parcels share one counter, so its Status admits the union of the
# two state machines. Append-only, like every ENUM here: a test checks that it
# equals the union of InputStatus and ParcelStatus as a set.
COUNTER_STATUSES = (
    "Unassigned",
    "Assigned",
    "Processed",
    "Split",
    "Failed",
    "Problematic",
    "NotProcessed",
    "Reserved",
    "Completing",
    "Done",
    "PartiallyDone",
    "Cancelled",
)

TRANSFORMATION_COUNTER = journalled_counter(
    Base.metadata,
    "Transformation",
    key=[
        ("TransformationID", BinaryUUID()),
        ("Entity", native_enum(CounterEntity)),
        ("Status", Enum(*COUNTER_STATUSES, name="counterstatus")),
    ],
    measures=["Count"],
)

DATA_PARCELS_COUNTER = journalled_counter(
    Base.metadata,
    "DataParcels",
    key=[
        ("TransformationID", BinaryUUID()),
        ("Status", native_enum(ParcelStatus)),
        ("StorageElement", String(64)),
    ],
    measures=["LFNCountAdded", "LFNSizeAdded", "LFNCountFreed", "LFNSizeFreed"],
)


class TransformationLog(Base):
    """Append-only audit trail of a transformation; survives cleaning.

    Attributes:
        log_id: Database-generated identifier.
        transformation_id: Transformation the event concerns.
        event_type: Kind of event, extensible by extensions.
        old_status: Status before a state change.
        new_status: Status after a state change.
        author: User or component responsible.
        message: Free-text message.
        data: Event detail.
        created_at: When the event was recorded.
    """

    __tablename__ = "TransformationLog"
    log_id: Mapped[int] = mapped_column(
        "LogID", BIGINT_AUTOINCREMENT, primary_key=True, autoincrement=True
    )
    transformation_id: Mapped[UUID] = mapped_column(
        "TransformationID", ForeignKey(Transformations.transformation_id)
    )
    event_type: Mapped[str32] = mapped_column("EventType")
    old_status: Mapped[TransformationStatus | None] = mapped_column(
        "OldStatus", native_enum(TransformationStatus)
    )
    new_status: Mapped[TransformationStatus | None] = mapped_column(
        "NewStatus", native_enum(TransformationStatus)
    )
    author: Mapped[str64] = mapped_column("Author")
    message: Mapped[str512 | None] = mapped_column("Message")
    data: Mapped[dict[str, Any] | None] = mapped_column("Data")
    created_at: Mapped[created_ts] = mapped_column("CreatedAt")

    __table_args__ = (Index("TransformationLogTime", "TransformationID", "CreatedAt"),)


class WorkgraphLog(Base):
    """Append-only audit trail of a workgraph; survives cleaning.

    Attributes:
        log_id: Database-generated identifier.
        workgraph_id: Workgraph the event concerns.
        event_type: Kind of event, extensible by extensions.
        old_status: Status before a state change.
        new_status: Status after a state change.
        author: User or component responsible.
        message: Free-text message.
        data: Event detail.
        created_at: When the event was recorded.
    """

    __tablename__ = "WorkgraphLog"
    log_id: Mapped[int] = mapped_column(
        "LogID", BIGINT_AUTOINCREMENT, primary_key=True, autoincrement=True
    )
    workgraph_id: Mapped[UUID] = mapped_column(
        "WorkgraphID", ForeignKey(Workgraphs.workgraph_id)
    )
    event_type: Mapped[str32] = mapped_column("EventType")
    old_status: Mapped[WorkgraphStatus | None] = mapped_column(
        "OldStatus", native_enum(WorkgraphStatus)
    )
    new_status: Mapped[WorkgraphStatus | None] = mapped_column(
        "NewStatus", native_enum(WorkgraphStatus)
    )
    author: Mapped[str64] = mapped_column("Author")
    message: Mapped[str512 | None] = mapped_column("Message")
    data: Mapped[dict[str, Any] | None] = mapped_column("Data")
    created_at: Mapped[created_ts] = mapped_column("CreatedAt")

    __table_args__ = (Index("WorkgraphLogTime", "WorkgraphID", "CreatedAt"),)
