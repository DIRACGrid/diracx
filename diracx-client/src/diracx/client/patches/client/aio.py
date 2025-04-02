"""Patches for the autorest-generated jobs client.

This file can be used to customize the generated code for the jobs client.
When adding new classes to this file, make sure to also add them to the
__all__ list in the corresponding file in the patches directory.
"""

from __future__ import annotations

__all__ = [
    "Dirac",
]

from ..._generated.aio._client import Dirac as _Dirac
from .common import DiracAuthMixin


class Dirac(DiracAuthMixin, _Dirac): ...
