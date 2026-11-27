from __future__ import annotations

import inspect

import pytest
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.models.transformation import PackerInput
from pydantic import ValidationError
from uuid_utils.compat import uuid7

from gubbins.logic.transformations.packers import TrivialPackerArgs, trivial_packer


async def pack(n_inputs: int, *, flush=False, feeder_active=True):
    """Pack ``n_inputs`` new inputs; return their IDs and the parcels made."""
    inputs = [PackerInput(input_id=uuid7()) for _ in range(n_inputs)]

    async def stream():
        for packer_input in inputs:
            yield packer_input

    parcels = [
        parcel
        async for parcel in trivial_packer(
            packer_args=TrivialPackerArgs(),
            inputs=stream(),
            flush=flush,
            feeder_active=feeder_active,
            input_overview={},
        )
    ]
    return [i.input_id for i in inputs], parcels


@pytest.mark.parametrize(
    "kwargs",
    [{}, {"flush": True}, {"feeder_active": False}],
    ids=["feeding", "flush", "drained"],
)
async def test_one_parcel_per_input_whatever_the_feeder(kwargs):
    ids, parcels = await pack(3, **kwargs)
    assert [parcel.input_ids for parcel in parcels] == [[i] for i in ids]


async def test_parcels_carry_nothing_but_their_input():
    _, [parcel] = await pack(1)
    assert parcel.metadata == {}
    assert parcel.requirements is None
    assert parcel.parameters == {}


async def test_no_inputs_no_parcels():
    assert await pack(0, flush=True) == ([], [])


def test_args_reject_unknown_keys():
    with pytest.raises(ValidationError):
        TrivialPackerArgs.model_validate({"group_size": 2})


def test_trivial_packer_is_registered():
    [entry_point] = select_from_extension(group=DiracEntryPoint.PACKERS, name="Trivial")
    packer = entry_point.load()
    assert packer is trivial_packer
    assert inspect.isasyncgenfunction(packer)
