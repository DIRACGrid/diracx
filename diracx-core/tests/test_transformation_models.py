from __future__ import annotations

import pytest

from diracx.core.models.transformation import (
    INPUT_TRANSITIONS,
    PARCEL_TRANSITIONS,
    TRANSFORMATION_TRANSITIONS,
    WORKGRAPH_MEMBER_STATUSES,
    WORKGRAPH_TRANSITIONS,
    ActionResult,
    CounterEntity,
    InputStatus,
    ParcelStatus,
    TransformationKind,
    TransformationStatus,
    WorkgraphStatus,
)

# The values are stored in native ENUM columns, where only appending a value
# avoids a table rebuild. Extend these lists at the end, never in the middle.
FROZEN_ORDER = {
    WorkgraphStatus: [
        "New", "Scouting", "Approving", "ApprovingBlocked", "Active",
        "Finalizing", "Completed", "Archiving", "Archived", "Cancelling",
        "Cancelled",
    ],
    TransformationStatus: [
        "New", "Active", "Paused", "Finalizing", "FinalizingBlocked",
        "Finalized", "Completed", "Archiving", "ArchivingBlocked", "Archived",
        "Cancelling", "CancellingBlocked", "Cancelled",
    ],
    InputStatus: [
        "Unassigned", "Assigned", "Processed", "Split", "Failed",
        "Problematic", "NotProcessed",
    ],
    ParcelStatus: [
        "Unassigned", "Reserved", "Assigned", "Completing", "Done",
        "PartiallyDone", "Failed", "Cancelled",
    ],
    TransformationKind: ["Compute", "Data"],
    ActionResult: ["Pending", "Passed", "Failed", "Done"],
    CounterEntity: ["Input", "Parcel"],
}  # fmt: skip


@pytest.mark.parametrize("enum_type", FROZEN_ORDER, ids=lambda e: e.__name__)
def test_enum_values_are_append_only(enum_type):
    values = [member.value for member in enum_type]
    frozen = FROZEN_ORDER[enum_type]
    assert values[: len(frozen)] == frozen, (
        "Existing values were reordered, renamed or removed; "
        "only appending is allowed (see the module docstring)"
    )


@pytest.mark.parametrize(
    "transitions, enum_type",
    [
        (WORKGRAPH_TRANSITIONS, WorkgraphStatus),
        (TRANSFORMATION_TRANSITIONS, TransformationStatus),
        (INPUT_TRANSITIONS, InputStatus),
        (PARCEL_TRANSITIONS, ParcelStatus),
        (WORKGRAPH_MEMBER_STATUSES, WorkgraphStatus),
    ],
)
def test_state_tables_cover_every_state(transitions, enum_type):
    assert set(transitions) == set(enum_type)
