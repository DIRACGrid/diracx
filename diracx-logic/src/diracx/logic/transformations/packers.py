"""Packer plugins shipped with DiracX (DX-ADR-006)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator

from pydantic import BaseModel, PositiveInt

from diracx.core.models.transformation import (
    ComputeParcel,
    InputStatus,
    PackerInput,
)


class FixedSizePackerArgs(BaseModel):
    """Arguments of the fixed-size packer.

    Attributes:
        group_size: Number of inputs in each parcel.
    """

    group_size: PositiveInt = 1


async def fixed_size_packer(
    *,
    packer_args: FixedSizePackerArgs,
    inputs: AsyncIterator[PackerInput],
    flush: bool = False,
    feeder_active: bool,
    input_overview: dict[InputStatus, int],
) -> AsyncGenerator[ComputeParcel, None]:
    """Pack inputs into parcels of ``group_size``, in the order offered.

    A last, smaller group is held back while more input may arrive, and packed
    once the feeder is off or on a flush.

    Args:
        packer_args: The packer's arguments.
        inputs: The claimed ``Unassigned`` inputs.
        flush: Whether to pack the last group even if it is small.
        feeder_active: Whether more inputs may still arrive.
        input_overview: Number of the transformation's inputs in each status.

    Yields:
        One compute parcel per group.
    """
    group = []
    async for packer_input in inputs:
        group.append(packer_input.input_id)
        if len(group) == packer_args.group_size:
            yield ComputeParcel(inputs=group)
            group = []
    if group and (flush or not feeder_active):
        yield ComputeParcel(inputs=group)
