# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""

from __future__ import annotations

__all__ = [
    "AuthOperations",
    "JobsOperations",
]  # Add all objects you want publicly available to users at this package level

from ...patches.auth.sync import AuthOperations
from ...patches.jobs.sync import JobsOperations

try:
    from ...patches.pilots.sync import PilotsOperations

    __all__.append("PilotsOperations")
except ImportError:
    pass


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """
