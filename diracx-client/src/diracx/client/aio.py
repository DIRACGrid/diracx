from __future__ import absolute_import

__all__ = [
    "AsyncDiracClient",
    "async_operations",
]

from ._generated.aio import Dirac
from ._generated.aio import operations as async_operations


class AsyncDiracClient(Dirac):
    pass
