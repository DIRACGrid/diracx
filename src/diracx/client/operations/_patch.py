# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
import io
import json
from typing import Any, List, Union

from azure.core.tracing.decorator import distributed_trace

from .. import models as _models
from ._operations import (
    AuthOperations as AuthOperationsGenerated,
    JobsOperations as JobsOperationsGenerated,
    JSON,
)

__all__: List[str] = [
    "AuthOperations",
    "JobsOperations",
]  # Add all objects you want publicly available to users at this package level


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


class AuthOperations(AuthOperationsGenerated):
    @distributed_trace
    def token(self, vo: str, **kwargs: Any) -> Union[Any, _models.HTTPValidationError]:
        raise NotImplementedError("TODO")


class JobsOperations(JobsOperationsGenerated):
    @distributed_trace
    def search(  # type: ignore[override]
        self,
        *,
        parameters: list[str] | None = None,
        search: list[str] | None = None,
        sort: list[str] | None = None,
        **kwargs: Any,
    ) -> List[JSON]:
        """TODO"""
        body = {}
        if parameters is not None:
            body["parameters"] = parameters
        if search is not None:
            body["search"] = search
        if sort is not None:
            body["sort"] = sort
        # TODO: The BytesIO here is only needed to satify the typing
        # Probably an autorest bug
        body_data = io.BytesIO(json.dumps(body).encode("utf-8"))
        return super().search(body_data, **kwargs)

    @distributed_trace
    def summary(  # type: ignore[override]
        self,
        *,
        grouping: list[str] | None = None,
        search: list[str] | None = None,
        **kwargs: Any,
    ) -> List[JSON]:
        """TODO"""
        body = {}
        if grouping is not None:
            body["grouping"] = grouping
        if search is not None:
            body["search"] = search
        # TODO: The BytesIO here is only needed to satify the typing
        # Probably an autorest bug
        body_data = io.BytesIO(json.dumps(body).encode("utf-8"))
        return super().search(body_data, **kwargs)
