"""Patches for the autorest-generated jobs client.

This file can be used to customize the generated code for the jobs client.
When adding new classes to this file, make sure to also add them to the
__all__ list in the corresponding file in the patches directory.
"""

from __future__ import annotations

__all__ = [
    "JobsOperations",
]

from typing import IO, Any, Dict, Union, Unpack, cast

from azure.core.tracing.decorator import distributed_trace

from ..._generated.operations._operations import JobsOperations as _JobsOperations
from diracx.client._generated.models._models import JobMetaData
from .common import make_search_body, make_summary_body, SearchKwargs, SummaryKwargs, prepare_body_for_patch

# We're intentionally ignoring overrides here because we want to change the interface.
# mypy: disable-error-code=override


class JobsOperations(_JobsOperations):
    @distributed_trace
    def search(self, **kwargs: Unpack[SearchKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return super().search(**make_search_body(**kwargs))

    @distributed_trace
    def summary(self, **kwargs: Unpack[SummaryKwargs]) -> list[dict[str, Any]]:
        """TODO"""
        return super().summary(**make_summary_body(**kwargs))

    @distributed_trace
    def patch_metadata(  # type: ignore[override]
        self,
        body: Union[Dict[str | int, JobMetaData | Dict[str, Any]], IO[bytes], bytes],
        **kwargs: Any,
    ) -> None:
        """Patch Metadata.

        Accepts job ids as str|int and metadata as dicts with pythonic or wire keys.
        Unknown keys are emitted via `additional_properties`.
        TODO: Remove this method once we have structured and known job parameters.
        """
        prepared = prepare_body_for_patch(body)
        # Cast to Any to accommodate generated signature expecting Dict[str, JobMetaData]
        # while still allowing bytes/IO at runtime.
        super().patch_metadata(cast(Any, prepared), **kwargs)
