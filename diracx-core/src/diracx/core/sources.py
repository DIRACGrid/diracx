"""Generic caching source abstractions.

Sources wrap a backend (database, git repository, ...) whose content changes
rarely compared to how often it is read, caching a revision identifier and the
content it points to. Concrete implementations live next to the backend they
read from (e.g. the resource status sources in diracx-logic).
"""

from __future__ import annotations

__all__ = [
    "AsyncCacheableSource",
    "CacheableSource",
    "Snapshot",
]

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Generic, TypeVar

from cachetools import Cache, LRUCache

from diracx.core.utils import AsyncTwoLevelCache, TwoLevelCache

T = TypeVar("T")

DEFAULT_CS_REV_CACHE_SOFT_TTL = 5
# TODO: Reduce the hard TTL when we have more redundancy around the source of truth
DEFAULT_CS_REV_CACHE_HARD_TTL = 60 * 60


@dataclass(frozen=True)
class Snapshot(Generic[T]):
    """Wraps a cached data payload with its cache metadata."""

    data: T
    hexsha: str
    modified: datetime


class CacheableSource(Generic[T], metaclass=ABCMeta):
    """Abstract base class for sources that can be cached.

    This class handles caching of the latest revision and its content using a
    two-level cache.
    """

    def __init__(self):
        # Revision cache is used to store the latest revision and its
        # modification date. This cache has two TTLs, one which triggers the
        # background refresh and the other which is results in a hard failure.
        # This allows us to avoid blocking while the refresh is done, while
        # maintaining strong guarantees on the data freshness.
        self._revision_cache = TwoLevelCache(
            soft_ttl=DEFAULT_CS_REV_CACHE_SOFT_TTL,
            hard_ttl=DEFAULT_CS_REV_CACHE_HARD_TTL,
            max_workers=1,
            max_items=1,
        )
        # The content of a given revision can be stored in a simple LRU cache
        # We keep the last two versions in memory to avoid any potential to flip
        # flop between two versions when it changes.
        self._content_cache: Cache = LRUCache(maxsize=2)

    @abstractmethod
    def latest_revision(self) -> tuple[str, datetime]:
        """Return the latest revision and its modification time.

        Returns:
            tuple[str, datetime]: A unique hash for the latest version and the
                datetime when the version was modified.
        """

    @abstractmethod
    def read_raw(self, hexsha: str, modified: datetime) -> T:
        """Return the source object for a specific revision.

        Args:
            hexsha (str): Unique hash identifying the source revision.
            modified (datetime): Modification time to attach to the source.

        Returns:
            T: The source object corresponding to the given revision.
        """

    def read(self) -> T:
        """Load the source from the backend with appropriate caching.

        Returns:
            T: The cached source object.

        Raises:
            diracx.core.exceptions.NotReadyError: If the source is still being loaded.
            git.exc.BadName: If the version does not exist.
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_work, blocking=True
        )
        return self._content_cache[hexsha]

    async def read_non_blocking(self) -> T:
        """Load the source from the backend with appropriate caching.

        Returns:
            T: The cached source object.

        Raises:
            diracx.core.exceptions.NotReadyError: If the source is still being loaded.
            git.exc.BadName: If the version does not exist.
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_work, blocking=False
        )
        return self._content_cache[hexsha]

    def _read_work(self) -> str:
        """Populate the caches for the latest revision.

        This function ensures that the latest revision is loaded into the
        content cache before it is admitted into the revision cache.

        Returns:
            str: The current revision hash.
        """
        hexsha, modified = self.latest_revision()
        if hexsha not in self._content_cache:
            self._content_cache[hexsha] = self.read_raw(hexsha, modified)
        return hexsha

    def clear_caches(self):
        """Clear the revision and content caches."""
        self._revision_cache.clear()
        self._content_cache.clear()


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
