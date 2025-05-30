# coding=utf-8
# --------------------------------------------------------------------------
# Code generated by Microsoft (R) AutoRest Code Generator (autorest: 3.10.4, generator: @autorest/python@6.34.2)
# Changes may cause incorrect behavior and will be lost if the code is regenerated.
# --------------------------------------------------------------------------
# pylint: disable=wrong-import-position

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._patch import *  # pylint: disable=unused-wildcard-import

from ._operations import WellKnownOperations  # type: ignore
from ._operations import AuthOperations  # type: ignore
from ._operations import ConfigOperations  # type: ignore
from ._operations import JobsOperations  # type: ignore
from ._operations import LollygagOperations  # type: ignore

from ._patch import __all__ as _patch_all
from ._patch import *
from ._patch import patch_sdk as _patch_sdk

__all__ = [
    "WellKnownOperations",
    "AuthOperations",
    "ConfigOperations",
    "JobsOperations",
    "LollygagOperations",
]
__all__.extend([p for p in _patch_all if p not in __all__])  # pyright: ignore
_patch_sdk()
