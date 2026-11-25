"""Packer that makes one parcel per input (DX-ADR-006).

The simplest packer there is, and the one to start from when writing your
own: every input offered becomes a parcel of its own, straight away. It suits
transformations whose inputs are independent units of work, such as a
simulation where each input is a seed.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Mapping

from diracx.core.models._base import BaseModel
from diracx.core.models.transformation import (
    ComputeParcel,
    InputStatus,
    PackerInput,
)
from pydantic import ConfigDict


class TrivialPackerArgs(BaseModel):
    """Arguments of the trivial packer: there are none.

    Any key is refused, so a transformation configured for another packer
    fails validation instead of silently ignoring its arguments.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


async def trivial_packer(
    *,
    packer_args: TrivialPackerArgs,
    inputs: AsyncIterator[PackerInput],
    flush: bool,
    feeder_active: bool,
    input_overview: Mapping[InputStatus, int],
) -> AsyncGenerator[ComputeParcel, None]:
    """Pack each input into a parcel of its own, in the order offered.

    Nothing is ever held back, so the result is the same whether or not the
    feeder is active or a flush was asked for.

    Args:
        packer_args: The packer's arguments; there are none.
        inputs: The claimed ``Unassigned`` inputs.
        flush: Part of the packer contract, unused: there is no small group to
            hold back.
        feeder_active: Part of the packer contract, unused for the same reason.
        input_overview: Part of the packer contract, unused as one input per
            parcel does not depend on how much of the pool is left.

    Yields:
        One compute parcel per input.
    """
    async for packer_input in inputs:
        yield ComputeParcel(input_ids=[packer_input.input_id])
