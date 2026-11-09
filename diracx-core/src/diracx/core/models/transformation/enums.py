"""Enumerations of the Transformation System (DX-ADR-004, DX-ADR-005).

The members of each status enum are written in state-machine order and new
states must only ever be appended: the database stores them as native
``ENUM`` columns, and appending a value is the only change that does not
rewrite rows.

The allowed transitions of each state machine are spelled out below as
dictionaries from a state to the states it may move to; a state mapping to
the empty set is terminal. They transcribe the diagrams of DX-ADR-005 and are
what ``diracx-logic`` checks a transition against. States an entity can be
created in directly (an input born ``Processed`` by a recovery split, a
recovery parcel born ``Done``) are creations, not transitions, so they do not
appear as targets.
"""

from __future__ import annotations

from enum import StrEnum


class WorkgraphStatus(StrEnum):
    """Lifecycle statuses for a workgraph."""

    NEW = "New"
    SCOUTING = "Scouting"
    APPROVING = "Approving"
    APPROVING_BLOCKED = "ApprovingBlocked"
    ACTIVE = "Active"
    FINALIZING = "Finalizing"
    COMPLETED = "Completed"
    ARCHIVING = "Archiving"
    ARCHIVED = "Archived"
    CANCELLING = "Cancelling"
    CANCELLED = "Cancelled"


class TransformationStatus(StrEnum):
    """Lifecycle statuses for a transformation."""

    NEW = "New"
    ACTIVE = "Active"
    PAUSED = "Paused"
    FINALIZING = "Finalizing"
    FINALIZING_BLOCKED = "FinalizingBlocked"
    FINALIZED = "Finalized"
    COMPLETED = "Completed"
    ARCHIVING = "Archiving"
    ARCHIVING_BLOCKED = "ArchivingBlocked"
    ARCHIVED = "Archived"
    CANCELLING = "Cancelling"
    CANCELLING_BLOCKED = "CancellingBlocked"
    CANCELLED = "Cancelled"


class InputStatus(StrEnum):
    """Lifecycle statuses for a transformation input."""

    UNASSIGNED = "Unassigned"
    ASSIGNED = "Assigned"
    PROCESSED = "Processed"
    SPLIT = "Split"
    FAILED = "Failed"
    PROBLEMATIC = "Problematic"
    NOT_PROCESSED = "NotProcessed"


class ParcelStatus(StrEnum):
    """Lifecycle statuses for a parcel."""

    UNASSIGNED = "Unassigned"
    RESERVED = "Reserved"
    ASSIGNED = "Assigned"
    COMPLETING = "Completing"
    DONE = "Done"
    PARTIALLY_DONE = "PartiallyDone"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


class TransformationKind(StrEnum):
    """What the parcels of a transformation become."""

    COMPUTE = "Compute"
    DATA = "Data"


class ActionResult(StrEnum):
    """Outcome of the last run of a lifecycle action."""

    PENDING = "Pending"
    PASSED = "Passed"
    FAILED = "Failed"
    DONE = "Done"


class CounterEntity(StrEnum):
    """Which population a row of the transformation counter counts."""

    INPUT = "Input"
    PARCEL = "Parcel"


WORKGRAPH_TRANSITIONS: dict[WorkgraphStatus, frozenset[WorkgraphStatus]] = {
    WorkgraphStatus.NEW: frozenset(
        {WorkgraphStatus.SCOUTING, WorkgraphStatus.ACTIVE, WorkgraphStatus.CANCELLING}
    ),
    WorkgraphStatus.SCOUTING: frozenset(
        {WorkgraphStatus.APPROVING, WorkgraphStatus.CANCELLING}
    ),
    WorkgraphStatus.APPROVING: frozenset(
        {
            WorkgraphStatus.APPROVING_BLOCKED,
            WorkgraphStatus.SCOUTING,
            WorkgraphStatus.ACTIVE,
            WorkgraphStatus.CANCELLING,
        }
    ),
    WorkgraphStatus.APPROVING_BLOCKED: frozenset(
        {
            WorkgraphStatus.APPROVING,
            WorkgraphStatus.SCOUTING,
            WorkgraphStatus.CANCELLING,
        }
    ),
    WorkgraphStatus.ACTIVE: frozenset(
        {WorkgraphStatus.FINALIZING, WorkgraphStatus.CANCELLING}
    ),
    WorkgraphStatus.FINALIZING: frozenset(
        {
            WorkgraphStatus.ACTIVE,
            WorkgraphStatus.COMPLETED,
            WorkgraphStatus.CANCELLING,
        }
    ),
    WorkgraphStatus.COMPLETED: frozenset({WorkgraphStatus.ARCHIVING}),
    WorkgraphStatus.ARCHIVING: frozenset({WorkgraphStatus.ARCHIVED}),
    WorkgraphStatus.ARCHIVED: frozenset(),
    WorkgraphStatus.CANCELLING: frozenset({WorkgraphStatus.CANCELLED}),
    WorkgraphStatus.CANCELLED: frozenset(),
}

TRANSFORMATION_TRANSITIONS: dict[
    TransformationStatus, frozenset[TransformationStatus]
] = {
    TransformationStatus.NEW: frozenset(
        {
            TransformationStatus.ACTIVE,
            TransformationStatus.PAUSED,
            TransformationStatus.CANCELLING,
        }
    ),
    TransformationStatus.ACTIVE: frozenset(
        {
            TransformationStatus.PAUSED,
            TransformationStatus.FINALIZING,
            TransformationStatus.CANCELLING,
        }
    ),
    TransformationStatus.PAUSED: frozenset(
        {
            TransformationStatus.ACTIVE,
            TransformationStatus.FINALIZING,
            TransformationStatus.CANCELLING,
        }
    ),
    TransformationStatus.FINALIZING: frozenset(
        {
            TransformationStatus.FINALIZING_BLOCKED,
            TransformationStatus.FINALIZED,
            TransformationStatus.ACTIVE,
            TransformationStatus.CANCELLING,
        }
    ),
    TransformationStatus.FINALIZING_BLOCKED: frozenset(
        {
            TransformationStatus.FINALIZING,
            TransformationStatus.ACTIVE,
            TransformationStatus.CANCELLING,
        }
    ),
    TransformationStatus.FINALIZED: frozenset(
        {
            TransformationStatus.ACTIVE,
            TransformationStatus.COMPLETED,
            TransformationStatus.CANCELLING,
        }
    ),
    TransformationStatus.COMPLETED: frozenset({TransformationStatus.ARCHIVING}),
    TransformationStatus.ARCHIVING: frozenset(
        {TransformationStatus.ARCHIVING_BLOCKED, TransformationStatus.ARCHIVED}
    ),
    TransformationStatus.ARCHIVING_BLOCKED: frozenset({TransformationStatus.ARCHIVING}),
    TransformationStatus.ARCHIVED: frozenset(),
    TransformationStatus.CANCELLING: frozenset(
        {TransformationStatus.CANCELLING_BLOCKED, TransformationStatus.CANCELLED}
    ),
    TransformationStatus.CANCELLING_BLOCKED: frozenset(
        {TransformationStatus.CANCELLING}
    ),
    TransformationStatus.CANCELLED: frozenset(),
}

INPUT_TRANSITIONS: dict[InputStatus, frozenset[InputStatus]] = {
    InputStatus.UNASSIGNED: frozenset(
        {InputStatus.ASSIGNED, InputStatus.PROBLEMATIC, InputStatus.NOT_PROCESSED}
    ),
    InputStatus.ASSIGNED: frozenset(
        {
            InputStatus.PROCESSED,
            InputStatus.SPLIT,
            InputStatus.FAILED,
            InputStatus.NOT_PROCESSED,
        }
    ),
    InputStatus.PROCESSED: frozenset(),
    InputStatus.SPLIT: frozenset(),
    InputStatus.FAILED: frozenset(
        {
            InputStatus.UNASSIGNED,
            InputStatus.SPLIT,
            InputStatus.PROBLEMATIC,
            InputStatus.NOT_PROCESSED,
        }
    ),
    InputStatus.PROBLEMATIC: frozenset(
        {InputStatus.UNASSIGNED, InputStatus.NOT_PROCESSED}
    ),
    InputStatus.NOT_PROCESSED: frozenset(),
}

PARCEL_TRANSITIONS: dict[ParcelStatus, frozenset[ParcelStatus]] = {
    ParcelStatus.UNASSIGNED: frozenset({ParcelStatus.RESERVED, ParcelStatus.CANCELLED}),
    ParcelStatus.RESERVED: frozenset(
        {ParcelStatus.ASSIGNED, ParcelStatus.UNASSIGNED, ParcelStatus.COMPLETING}
    ),
    ParcelStatus.ASSIGNED: frozenset({ParcelStatus.COMPLETING, ParcelStatus.FAILED}),
    ParcelStatus.COMPLETING: frozenset(
        {
            ParcelStatus.DONE,
            ParcelStatus.PARTIALLY_DONE,
            ParcelStatus.CANCELLED,
            ParcelStatus.FAILED,
        }
    ),
    ParcelStatus.DONE: frozenset(),
    ParcelStatus.PARTIALLY_DONE: frozenset(),
    ParcelStatus.FAILED: frozenset(),
    ParcelStatus.CANCELLED: frozenset(),
}

# The states the member transformations of a workgraph may be in, for each
# state of the workgraph
WORKGRAPH_MEMBER_STATUSES: dict[WorkgraphStatus, frozenset[TransformationStatus]] = {
    WorkgraphStatus.NEW: frozenset({TransformationStatus.NEW}),
    WorkgraphStatus.SCOUTING: frozenset(
        {TransformationStatus.ACTIVE, TransformationStatus.PAUSED}
    ),
    WorkgraphStatus.APPROVING: frozenset(
        {TransformationStatus.ACTIVE, TransformationStatus.PAUSED}
    ),
    WorkgraphStatus.APPROVING_BLOCKED: frozenset(
        {TransformationStatus.ACTIVE, TransformationStatus.PAUSED}
    ),
    WorkgraphStatus.ACTIVE: frozenset(
        {TransformationStatus.ACTIVE, TransformationStatus.PAUSED}
    ),
    WorkgraphStatus.FINALIZING: frozenset(
        {
            TransformationStatus.FINALIZING,
            TransformationStatus.FINALIZING_BLOCKED,
            TransformationStatus.FINALIZED,
        }
    ),
    WorkgraphStatus.COMPLETED: frozenset({TransformationStatus.COMPLETED}),
    WorkgraphStatus.ARCHIVING: frozenset(
        {
            TransformationStatus.ARCHIVING,
            TransformationStatus.ARCHIVING_BLOCKED,
            TransformationStatus.ARCHIVED,
        }
    ),
    WorkgraphStatus.ARCHIVED: frozenset({TransformationStatus.ARCHIVED}),
    WorkgraphStatus.CANCELLING: frozenset(
        {
            TransformationStatus.CANCELLING,
            TransformationStatus.CANCELLING_BLOCKED,
            TransformationStatus.CANCELLED,
        }
    ),
    WorkgraphStatus.CANCELLED: frozenset({TransformationStatus.CANCELLED}),
}
