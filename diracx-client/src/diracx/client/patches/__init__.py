from __future__ import annotations

from typing import TYPE_CHECKING

from diracx.core.extensions import select_from_extension

from .utils import DiracClientMixin


# If we're doing static analysis assume that the client class is the one from
# the current Python module.
if TYPE_CHECKING:
    from diracx.client.generated._client import Dirac as DiracGenerated
else:
    DiracGenerated = select_from_extension(group="diracx", name="client_class")[
        0
    ].load()


__all__: list[str] = [
    "DiracClient",
]  # Add all objects you want publicly available to users at this package level


class DiracClient(DiracClientMixin, DiracGenerated): ...
