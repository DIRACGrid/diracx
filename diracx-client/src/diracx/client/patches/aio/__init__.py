from __future__ import annotations

from typing import List, TYPE_CHECKING

from .utils import DiracClientMixin

if TYPE_CHECKING:
    from diracx.client.generated.aio._client import Dirac

from diracx.core.extensions import select_from_extension

real_client = select_from_extension(group="diracx", name="aio_client_class")[0].load()
DiracGenerated: type[Dirac] = real_client

__all__: List[str] = [
    "DiracClient",
]  # Add all objects you want publicly available to users at this package level


class DiracClient(DiracClientMixin, DiracGenerated): ...  # type: ignore
