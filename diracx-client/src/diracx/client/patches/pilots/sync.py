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

from azure.core.tracing.decorator import distributed_trace

from ..._generated.operations._operations import PilotsOperations as _PilotsOperations
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
    @distributed_trace
    def search(self, **kwargs: Unpack[SearchKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return super().search(**make_search_body(**kwargs))

    @distributed_trace
    def summary(self, **kwargs: Unpack[SummaryKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return super().summary(**make_summary_body(**kwargs))

    @distributed_trace
    def add_pilot_stamps(self, **kwargs: Unpack[AddPilotStampsKwargs]) -> None:
        """TODO"""
        return super().add_pilot_stamps(**make_add_pilot_stamps_body(**kwargs))

    @distributed_trace
    def update_pilot_fields(self, **kwargs: Unpack[UpdatePilotFieldsKwargs]) -> None:
        """TODO"""
        return super().update_pilot_fields(**make_update_pilot_fields_body(**kwargs))
