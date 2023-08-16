# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""


from typing import Any, List, Union

from azure.core.tracing.decorator import distributed_trace

from .. import models as _models
from ._operations import AuthOperations as AuthOperationsGenerated

__all__: list[str] = ["AuthOperations"]  # Add all objects you want publicly available to users at this package level


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


class AuthOperations(AuthOperationsGenerated):
    @distributed_trace
    def token(self, vo: str, **kwargs: Any) -> Any | _models.HTTPValidationError:
        raise NotImplementedError("TODO")
