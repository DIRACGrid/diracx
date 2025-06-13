"""Patches for the autorest-generated pilots client.

This file can be used to customize the generated code for the pilots client.
When adding new classes to this file, make sure to also add them to the
__all__ list in the corresponding file in the patches directory.
"""

from __future__ import annotations

__all__ = [
    "PilotsOperations",
]

from typing import Any, Unpack

from azure.core.tracing.decorator_async import distributed_trace_async

from ..._generated.aio.operations._operations import PilotsOperations as _PilotsOperations
from ..._generated.models._models import PilotCredentialsInfo
from .common import (
    make_search_body,
    make_summary_body,
    make_add_pilot_stamps_body,
    make_update_pilot_fields_body,
    SearchKwargs,
    SummaryKwargs,
    AddPilotStampsKwargs,
    UpdatePilotFieldsKwargs
)

# We're intentionally ignoring overrides here because we want to change the interface.
# mypy: disable-error-code=override


class PilotsOperations(_PilotsOperations):
    @distributed_trace_async
    async def search(self, **kwargs: Unpack[SearchKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return await super().search(**make_search_body(**kwargs))

    @distributed_trace_async
    async def summary(self, **kwargs: Unpack[SummaryKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return await super().summary(**make_summary_body(**kwargs))

    @distributed_trace_async
    async def add_pilot_stamps(self, **kwargs: Unpack[AddPilotStampsKwargs]) -> list[PilotCredentialsInfo] | None:
        """TODO"""
        return await super().add_pilot_stamps(**make_add_pilot_stamps_body(**kwargs))

    @distributed_trace_async
    async def update_pilot_fields(self, **kwargs: Unpack[UpdatePilotFieldsKwargs]) -> None:
        """TODO"""
        return await super().update_pilot_fields(**make_update_pilot_fields_body(**kwargs))
