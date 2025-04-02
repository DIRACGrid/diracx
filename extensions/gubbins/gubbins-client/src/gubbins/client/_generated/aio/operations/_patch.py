# pylint: disable=line-too-long,useless-suppression
# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
# bak = gubbins.client._generated.aio.operations._operations.WellKnownOperations
# gubbins.client._generated.aio.operations._operations.WellKnownOperations = gubbins.client._generated.aio.operations.WellKnownOperations
# # ------------------------------------

from typing import List

from ._operations import WellKnownOperations as _WellKnownOperations

# from diracx.client._generated.aio.operations._patch import *
# from diracx.client._generated.aio.operations._patch import __all__ as _patch_all


class WellKnownOperations(_WellKnownOperations):
    def my_method(self): ...


__all__ = ["WellKnownOperations"]
# __all__ += [x for x in _patch_all if x not in __all__]


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


# # ------------------------------------
# gubbins.client._generated.aio.operations._operations.WellKnownOperations = bak
# apply_patches_from_above()


# WellKnownOperations -> gubbins.client..._patch -> diracx.client..._patch -> gubbins.client..._operations -> object


# gubbins.client...__init__
#  * gubbins.client..._operations
#  * diracx.client..._patch (asked for gubbins.client..._patch but metapathfinder changed it)
#  *
