"""Patches for the autorest-generated sync pilots client."""

from __future__ import annotations

__all__ = [
    "PilotsOperations",
]

from typing import Any, Unpack

from azure.core.tracing.decorator import distributed_trace

from ..._generated.operations._operations import PilotsOperations as _PilotsOperations
from .common import (
    RegisterPilotsKwargs,
    SearchKwargs,
    SummaryKwargs,
    make_register_pilots_body,
    make_search_body,
    make_summary_body,
)

# We're intentionally ignoring overrides here because we want to change the interface.
# mypy: disable-error-code=override


class PilotsOperations(_PilotsOperations):
    @distributed_trace
    def search(self, **kwargs: Unpack[SearchKwargs]) -> list[dict[str, Any]]:
        """Search for pilots matching the provided filters."""
        return super().search(**make_search_body(**kwargs))

    @distributed_trace
    def summary(self, **kwargs: Unpack[SummaryKwargs]) -> list[dict[str, Any]]:
        """Return pilot counts aggregated by the requested columns."""
        return super().summary(**make_summary_body(**kwargs))

    @distributed_trace
    def register_pilots(self, **kwargs: Unpack[RegisterPilotsKwargs]) -> None:
        """Register a batch of pilots."""
        return super().register_pilots(**make_register_pilots_body(**kwargs))
