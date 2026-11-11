"""Packer that groups inputs into parcels of a fixed size (DX-ADR-006)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from uuid import UUID

from pydantic import PositiveInt

from diracx.core.models._base import BaseModel
from diracx.core.models.transformation import (
    ComputeParcel,
    InputStatus,
    PackerInput,
)


class FixedSizePackerArgs(BaseModel):
    """Arguments of the fixed-size packer."""

    group_size: PositiveInt = 1
    """Number of inputs in each parcel."""


async def fixed_size_packer(
    *,
    packer_args: FixedSizePackerArgs,
    inputs: AsyncIterator[PackerInput],
    flush: bool,
    feeder_active: bool,
    input_overview: dict[InputStatus, int],
) -> AsyncGenerator[ComputeParcel, None]:
    """Pack inputs into parcels of ``group_size``, in the order offered.

    A last, smaller group is held back while more inputs may arrive. It is
    packed only on a flush, or once the feeder has stopped.

    Args:
        packer_args: The packer's arguments.
        inputs: The claimed ``Unassigned`` inputs.
        flush: Whether to pack the last group even if it is small.
        feeder_active: Whether more inputs may still arrive.
        input_overview: Number of the transformation's inputs in each status;
            part of the packer contract, unused here as the group size does
            not depend on how much of the pool is left.

    Yields:
        One compute parcel per group.
    """
    group: list[UUID] = []
    async for packer_input in inputs:
        group.append(packer_input.input_id)
        if len(group) == packer_args.group_size:
            yield ComputeParcel(input_ids=group)
            group = []
    if group and (flush or not feeder_active):
        yield ComputeParcel(input_ids=group)
