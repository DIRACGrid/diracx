from __future__ import annotations

__all__ = [
    "Dirac",
]

from diracx.client._generated._client import Dirac as _Dirac

try:
    from diracx.client._patches import (  # type: ignore[attr-defined]
        Dirac as _DiracPatch,
    )
except ImportError:

    class _DiracPatch:  # type: ignore[no-redef]
        pass


class Dirac(_DiracPatch, _Dirac):
    pass


def patch_sdk():
    pass
