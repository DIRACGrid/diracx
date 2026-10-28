"""Parcel status transitions and their effect on inputs (DX-ADR-005).

``diracx-db`` only moves rows; this module decides whether a move is allowed
and what else it implies, because the state machines are enforced here rather
than in the database (DX-ADR-005, Rejected Ideas).

Every change made by :func:`set_parcel_status` happens on the connection of the
``TransformationDB`` it is given, so the parcel transition and the input
transitions it implies commit or roll back together.
"""

from __future__ import annotations

import logging
from collections.abc import Collection

from uuid_utils import UUID

from diracx.core.exceptions import InvalidParcelTransitionError
from diracx.core.models.transformation import (
    PARCEL_TRANSITIONS,
    InputStatus,
    ParcelStatus,
)
from diracx.db.sql import TransformationDB

logger = logging.getLogger(__name__)

# What a parcel reaching a final status does to the inputs it was made of
# ("How the machines connect", DX-ADR-005). Only ``Assigned`` inputs move: an
# input in any other status has already left this parcel behind.
#   - Done: each input was processed by exactly this parcel.
#   - Failed: the inputs go to ``Failed`` with ``ErrorCount += 1``; the input
#     failure hook (DX-ADR-006) decides retry, give up or split later.
#   - Cancelled: no other parcel will claim the inputs, so they are written off.
# ``PartiallyDone`` needs the recovery split and is handled separately.
_INPUT_OUTCOMES: dict[ParcelStatus, tuple[InputStatus, bool]] = {
    ParcelStatus.DONE: (InputStatus.PROCESSED, False),
    ParcelStatus.FAILED: (InputStatus.FAILED, True),
    ParcelStatus.CANCELLED: (InputStatus.NOT_PROCESSED, False),
}


async def set_parcel_status(
    parcel_ids: Collection[UUID],
    from_status: ParcelStatus,
    to_status: ParcelStatus,
    *,
    transformation_db: TransformationDB,
    backend: str | None = None,
) -> list[UUID]:
    """Move parcels between two statuses of the parcel state machine.

    The transition is a compare-and-set: only parcels still in
    ``from_status`` move, and the others are skipped without error. A
    caller racing another worker for the same parcels therefore never moves
    a parcel twice, and finds out which ones it won from the return value.

    When the target status is final, the parcels' inputs are moved in the
    same transaction, following DX-ADR-005.

    Args:
        parcel_ids: Parcels to move.
        from_status: Status the parcels must currently be in.
        to_status: Status to move them to.
        transformation_db: Database holding the parcels, already entered.
        backend: Backend to record on the parcels, set by the dispatcher
            when it claims them (DX-ADR-003).

    Returns:
        The identifiers of the parcels that were moved.

    Raises:
        ValueError: If a parcel identifier is given more than once.
        InvalidParcelTransitionError: If the state machine has no edge from
            ``from_status`` to ``to_status``.
        NotImplementedError: For ``PartiallyDone``, whose recovery split
            (DX-ADR-003) is not implemented yet.
    """
    if len(set(parcel_ids)) != len(parcel_ids):
        # The number of parcels skipped would be wrong
        raise ValueError("Each parcel identifier must be given only once")
    if to_status not in PARCEL_TRANSITIONS[from_status]:
        raise InvalidParcelTransitionError(from_status, to_status)
    if to_status == ParcelStatus.PARTIALLY_DONE:
        # Moving the inputs alone would break "each input is processed by
        # exactly one parcel": the succeeded portions must be claimed by a
        # born-Done recovery parcel, which needs the job's status report
        raise NotImplementedError("The recovery split is not implemented yet")

    moved = await transformation_db.set_parcel_status(
        parcel_ids, from_status, to_status, backend=backend
    )
    if skipped := len(parcel_ids) - len(moved):
        logger.debug(
            "%d parcel(s) were no longer %s and were not moved to %s",
            skipped,
            from_status,
            to_status,
        )

    if moved and to_status in _INPUT_OUTCOMES:
        input_status, increment_error_count = _INPUT_OUTCOMES[to_status]
        await transformation_db.set_parcel_input_status(
            moved,
            InputStatus.ASSIGNED,
            input_status,
            increment_error_count=increment_error_count,
        )
    return moved
