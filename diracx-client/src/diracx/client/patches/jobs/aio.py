"""Patches for the autorest-generated jobs client.

This file can be used to customize the generated code for the jobs client.
When adding new classes to this file, make sure to also add them to the
__all__ list in the corresponding file in the patches directory.
"""

from __future__ import annotations

__all__ = [
    "JobsOperations",
]

from typing import Any, Unpack

from azure.core.tracing.decorator_async import distributed_trace_async

from ..._generated.aio.operations._operations import JobsOperations as _JobsOperations
from .common import make_search_body, make_summary_body, SearchKwargs, SummaryKwargs

# We're intentionally ignoring overrides here because we want to change the interface.
# mypy: disable-error-code=override


class JobsOperations(_JobsOperations):
    @distributed_trace_async
    async def search(self, **kwargs: Unpack[SearchKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return await super().search(**make_search_body(**kwargs))

    @distributed_trace_async
    async def summary(self, **kwargs: Unpack[SummaryKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return await super().summary(**make_summary_body(**kwargs))
