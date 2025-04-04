from __future__ import absolute_import

__all__ = [
    "AsyncGubbinsClient",
    "async_operations",
]

from ._generated.aio import Dirac
from ._generated.aio import operations as async_operations


class AsyncGubbinsClient(Dirac):
    pass


AsyncDiracClient = AsyncGubbinsClient
