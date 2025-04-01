from __future__ import annotations

__all__ = [
    "Dirac",
]

from gubbins.client._generated.aio._client import Dirac as _Dirac

try:
    from diracx.client._patches.aio import (  # type: ignore[attr-defined]
        Dirac as _DiracPatch,
    )
except ImportError:

    class _DiracPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.aio import (  # type: ignore[attr-defined]
        Dirac as _DiracPatchExt,
    )
except ImportError:

    class _DiracPatchExt:  # type: ignore[no-redef]
        pass


class Dirac(_DiracPatchExt, _DiracPatch, _Dirac):
    pass


def patch_sdk():
    pass
