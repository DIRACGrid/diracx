"""Generic caching source abstractions.

Sources wrap a backend (database, git repository, ...) whose content changes
rarely compared to how often it is read, caching a revision identifier and the
content it points to. Concrete implementations live next to the backend they
read from (e.g. the resource status sources in diracx-logic).
"""

from __future__ import annotations

__all__ = [
    "AsyncCacheableSource",
    "Snapshot",
]

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Generic, TypeVar

from cachetools import Cache, LRUCache

from diracx.core.utils import AsyncTwoLevelCache

T = TypeVar("T")


@dataclass(frozen=True)
class Snapshot(Generic[T]):
    """Wraps a cached data payload with its cache metadata."""

    data: T
    hexsha: str
    modified: datetime


class AsyncCacheableSource(Generic[T], metaclass=ABCMeta):
    """Abstract base class for async sources that can be cached.

    Async equivalent of CacheableSource. Uses AsyncTwoLevelCache so populate
    functions are native coroutines.
    """

    #: The database class this source reads from. Used by the application
    #: factory to instantiate the source with the matching database instance.
    db_class: ClassVar[type]

    #: TTLs for the revision cache: past the soft TTL a background refresh is
    #: triggered while the cached value is still served; past the hard TTL the
    #: data is considered too stale to serve. Subclasses can override these.
    rev_cache_soft_ttl: ClassVar[int] = 5
    # TODO: Reduce the hard TTL when we have more redundancy around the source of truth
    rev_cache_hard_ttl: ClassVar[int] = 60 * 60

    def __init__(self):
        self._revision_cache = AsyncTwoLevelCache(
            soft_ttl=self.rev_cache_soft_ttl,
            hard_ttl=self.rev_cache_hard_ttl,
            max_items=1,
        )
        self._content_cache: Cache = LRUCache(maxsize=2)

    @abstractmethod
    async def latest_revision(self) -> tuple[str, datetime]:
        """Return (revision_str, modified) identifying the current revision."""

    @abstractmethod
    async def read_raw(self, hexsha: str, modified: datetime) -> T:
        """Fetch and return the data for the given revision."""

    async def _read_work(self) -> str:
        hexsha, modified = await self.latest_revision()
        if hexsha not in self._content_cache:
            self._content_cache[hexsha] = await self.read_raw(hexsha, modified)
        return hexsha

    async def read(self) -> T:
        """Blocking read — awaits refresh on a hard cache miss."""
        hexsha = await self._revision_cache.get(
            "latest_revision", self._read_work, blocking=True
        )
        return self._content_cache[hexsha]

    async def read_non_blocking(self) -> T:
        """Non-blocking read — raises NotReadyError on a hard cache miss."""
        hexsha = await self._revision_cache.get(
            "latest_revision", self._read_work, blocking=False
        )
        return self._content_cache[hexsha]

    async def clear_caches(self):
        """Clear the caches."""
        await self._revision_cache.clear()
        self._content_cache.clear()

    @classmethod
    async def create(cls) -> T:
        """Dependency injection stub.

        The application factory instantiates each concrete source and
        overrides ``cls.create`` with the instance's ``read`` method, so this
        should never actually be called. Each subclass's bound ``create``
        classmethod is a distinct dependency key.
        """
        raise NotImplementedError(f"{cls.__name__} was not wired by the factory")
