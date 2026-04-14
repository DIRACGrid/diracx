"""Patches for the autorest-generated async pilots client."""

from __future__ import annotations

__all__ = [
    "PilotsOperations",
]

from typing import Any, Unpack

from azure.core.tracing.decorator_async import distributed_trace_async

from ..._generated.aio.operations._operations import (
    PilotsOperations as _PilotsOperations,
)
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
    @distributed_trace_async
    async def search(
        self, **kwargs: Unpack[SearchKwargs]
    ) -> list[dict[str, Any]]:
        """Search for pilots matching the provided filters."""
        return await super().search(**make_search_body(**kwargs))

    @distributed_trace_async
    async def summary(
        self, **kwargs: Unpack[SummaryKwargs]
    ) -> list[dict[str, Any]]:
        """Return pilot counts aggregated by the requested columns."""
        return await super().summary(**make_summary_body(**kwargs))

    @distributed_trace_async
    async def register_pilots(
        self, **kwargs: Unpack[RegisterPilotsKwargs]
    ) -> None:
        """Register a batch of pilots."""
        return await super().register_pilots(**make_register_pilots_body(**kwargs))
