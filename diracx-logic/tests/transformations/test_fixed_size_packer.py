from __future__ import annotations

import pytest
from pydantic import ValidationError
from uuid_utils.compat import uuid7

from diracx.core.models.transformation import PackerInput
from diracx.logic.transformations.packers import (
    FixedSizePackerArgs,
    fixed_size_packer,
)


async def pack(n_inputs: int, group_size: int, *, flush=False, feeder_active=True):
    """Pack ``n_inputs`` new inputs; return their IDs and the IDs in each parcel."""
    inputs = [PackerInput(input_id=uuid7()) for _ in range(n_inputs)]

    async def stream():
        for packer_input in inputs:
            yield packer_input

    parcels = [
        parcel.input_ids
        async for parcel in fixed_size_packer(
            packer_args=FixedSizePackerArgs(group_size=group_size),
            inputs=stream(),
            flush=flush,
            feeder_active=feeder_active,
            input_overview={},
        )
    ]
    return [i.input_id for i in inputs], parcels


async def test_full_groups_only_while_feeder_active():
    ids, parcels = await pack(7, 3)
    assert parcels == [ids[0:3], ids[3:6]]


@pytest.mark.parametrize(
    "kwargs", [{"flush": True}, {"feeder_active": False}], ids=["flush", "drained"]
)
async def test_remainder_packed_when_draining(kwargs):
    ids, parcels = await pack(7, 3, **kwargs)
    assert parcels == [ids[0:3], ids[3:6], ids[6:]]


async def test_exact_multiple_leaves_no_empty_parcel():
    ids, parcels = await pack(6, 3, flush=True)
    assert parcels == [ids[0:3], ids[3:6]]


async def test_no_inputs_no_parcels():
    assert await pack(0, 3, flush=True) == ([], [])


def test_group_size_must_be_positive():
    with pytest.raises(ValidationError):
        FixedSizePackerArgs(group_size=0)
