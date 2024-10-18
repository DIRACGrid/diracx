# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
from typing import Any, List

# Add all objects you want publicly available to users at this package level
__all__: List[str] = ["DeviceFlowErrorResponse"]

from .. import _serialization
from . import _models


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


class DeviceFlowErrorResponse(_serialization.Model):
    """TokenResponse.

    All required parameters must be populated in order to send to Azure.

    :ivar access_token: Access Token. Required.
    :vartype access_token: str
    :ivar expires_in: Expires In. Required.
    :vartype expires_in: int
    :ivar state: State. Required.
    :vartype state: str
    """

    _validation = {
        "error": {"required": True},
    }

    _attribute_map = {
        "error": {"key": "error", "type": "str"},
    }

    def __init__(self, *, error: str, **kwargs: Any) -> None:
        """
        :keyword error: Access Token. Required.
        :paramtype error: str
        """
        super().__init__(**kwargs)
        self.error = error
