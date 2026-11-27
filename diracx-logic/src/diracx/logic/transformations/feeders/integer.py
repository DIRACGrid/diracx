"""Monotonic integer feeder plugin (DX-ADR-006)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

from pydantic import PositiveInt

from diracx.core.models._base import BaseModel
from diracx.core.models.transformation import (
    FeederInput,
    NoMoreFeederInputs,
    SetFeederState,
)


class IntegerFeederArgs(BaseModel, allow_inf_nan=False):
    """Arguments of the monotonic integer feeder."""

    max_count: PositiveInt
    """Exclusive upper bound on seeds; exhaustion when ``next_seed >= max_count``.

    Required: without it the feeder would issue nothing and disable itself.
    """


async def integer_feeder(
    *,
    feeder_args: IntegerFeederArgs,
    feeder_state: dict[str, Any],
    scouting: bool,
) -> AsyncGenerator[FeederInput | SetFeederState | NoMoreFeederInputs, None]:
    """Yield consecutive integer seeds until ``max_count`` is reached.

    The bookmark is ``next_seed``: the next integer to issue. ``scouting`` is
    accepted for the feeder contract and ignored (MVP has no scouting branch).

    Args:
        feeder_args: Validated feeder arguments.
        feeder_state: Opaque bookmark from ``Transformations.FeederState``.
        scouting: Whether the workgraph is in scouting (unused).

    Yields:
        ``FeederInput`` proposals, a final ``SetFeederState``, then
        ``NoMoreFeederInputs`` once the range is covered. If already exhausted,
        only ``NoMoreFeederInputs``.
    """
    # NOTE: "contract kwarg"; no scouting behaviour in the MVP feeder
    next_seed = int(feeder_state.get("next_seed", 0))
    if next_seed >= feeder_args.max_count:
        yield NoMoreFeederInputs()
        return

    for seed in range(next_seed, feeder_args.max_count):
        yield FeederInput(descriptor={"seed": seed})
    yield SetFeederState(state={"next_seed": feeder_args.max_count})
    yield NoMoreFeederInputs()
