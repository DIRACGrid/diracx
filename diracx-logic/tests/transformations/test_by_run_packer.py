"""The example packer of docs/dev/explanations/transformation-system/packer.md.

The docs include the section between the ``--8<--`` markers, so the example
is tested here.
"""

from __future__ import annotations

# --8<-- [start:by_run_packer]
from collections import defaultdict
from collections.abc import AsyncGenerator, AsyncIterator
from uuid import UUID

from pydantic import BaseModel, PositiveInt

from diracx.core.models.transformation import (
    BadInput,
    ComputeParcel,
    InputStatus,
    PackerInput,
)


class ByRunPackerArgs(BaseModel):
    group_size: PositiveInt = 10


async def by_run_packer(
    *,
    packer_args: ByRunPackerArgs,
    inputs: AsyncIterator[PackerInput],
    flush: bool,
    feeder_active: bool,
    input_overview: dict[InputStatus, int],
) -> AsyncGenerator[ComputeParcel | BadInput, None]:
    groups: defaultdict[int, list[UUID]] = defaultdict(list)
    async for packer_input in inputs:
        run = packer_input.descriptor.get("Run")
        if not isinstance(run, int):
            yield BadInput(input_id=packer_input.input_id, reason="No run number")
            continue
        groups[run].append(packer_input.input_id)
        if len(groups[run]) == packer_args.group_size:
            yield ComputeParcel(input_ids=groups.pop(run), metadata={"Run": run})
    if flush or not feeder_active:
        for run, group in groups.items():
            yield ComputeParcel(input_ids=group, metadata={"Run": run})


# --8<-- [end:by_run_packer]


async def pack(descriptors, *, group_size=2, flush=False, feeder_active=True):
    from uuid_utils.compat import uuid7

    inputs = [PackerInput(input_id=uuid7(), descriptor=d) for d in descriptors]

    async def stream():
        for packer_input in inputs:
            yield packer_input

    proposals = [
        p
        async for p in by_run_packer(
            packer_args=ByRunPackerArgs(group_size=group_size),
            inputs=stream(),
            flush=flush,
            feeder_active=feeder_active,
            input_overview={},
        )
    ]
    return [i.input_id for i in inputs], proposals


async def test_groups_inputs_by_run():
    ids, proposals = await pack([{"Run": 1}, {"Run": 2}, {"Run": 1}, {"Run": 2}])
    assert proposals == [
        ComputeParcel(input_ids=[ids[0], ids[2]], metadata={"Run": 1}),
        ComputeParcel(input_ids=[ids[1], ids[3]], metadata={"Run": 2}),
    ]


async def test_input_without_run_is_bad():
    ids, proposals = await pack([{}, {"Run": "x"}])
    assert proposals == [
        BadInput(input_id=ids[0], reason="No run number"),
        BadInput(input_id=ids[1], reason="No run number"),
    ]


async def test_incomplete_run_held_back_while_feeder_active():
    _, proposals = await pack([{"Run": 1}])
    assert proposals == []


async def test_incomplete_run_packed_on_flush_or_without_feeder():
    for kwargs in ({"flush": True}, {"feeder_active": False}):
        ids, proposals = await pack([{"Run": 1}], **kwargs)
        assert proposals == [ComputeParcel(input_ids=ids, metadata={"Run": 1})]
