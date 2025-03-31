from __future__ import annotations

from typing import List

from .utils import (
    DiracClientMixin,
    AuthOperations,
    JobsOperations,
    DeviceFlowErrorResponse,
)


from diracx.core.extensions import select_from_extension

real_client = select_from_extension(group="diracx", name="client_class")[0].load()
DiracGenerated = real_client


__all__: List[str] = [
    "DiracClient",
    "AuthOperations",
    "JobsOperations",
    "DeviceFlowErrorResponse",
]  # Add all objects you want publicly available to users at this package level


class DiracClient(DiracClientMixin, DiracGenerated): ...  # type: ignore
