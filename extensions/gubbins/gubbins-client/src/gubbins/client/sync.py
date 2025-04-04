from __future__ import absolute_import

__all__ = [
    "SyncGubbinsClient",
    "sync_operations",
]

from ._generated import Dirac
from ._generated import operations as sync_operations


class SyncGubbinsClient(Dirac):
    pass


SyncDiracClient = SyncGubbinsClient
