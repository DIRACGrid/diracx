# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
from __future__ import annotations

from .. import _serialization

# Add all objects you want publicly available to users at this package level
__all__ = [
    "DeviceFlowErrorResponse",
]

from typing import Any


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


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """
