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
    """Wrap a cached data payload with its cache metadata.

    Attributes:
        data: Cached source data.
        hexsha: Identifier of the revision represented by the data.
        modified: Time associated with the revision.
    """

    data: T
    hexsha: str
    modified: datetime


class CacheableSource(Generic[T], metaclass=ABCMeta):
    """Abstract base class for sources that can be cached.

    Handles the caching of the latest revision and its content using a two-level cache.

    Attributes:
        _revision_cache: Cache containing the latest revision metadata.
        _content_cache: Cache containing source content by revision.
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
            A tuple containing a unique revision identifier and its timestamp.
        """

    @abstractmethod
    def read_raw(self, hexsha: str, modified: datetime) -> T:
        """Read source data for a specific revision.

        Args:
            hexsha: Identifier of the requested revision.
            modified: Time associated with the requested revision.

        Returns:
            The source data corresponding to the revision.
        """

    def read(self) -> T:
        """Load the source from the backend with appropriate caching.

        Returns:
            The latest source data.
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_work, blocking=True
        )
        return self._content_cache[hexsha]

    async def read_non_blocking(self) -> T:
        """Load the source from the backend with appropriate caching.

        Returns:
            The latest source data, potentially after a background refresh.
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_work, blocking=False
        )
        return self._content_cache[hexsha]

    def _read_work(self) -> str:
        """Work function for the thread pool of `self._revision_cache`.

        This function ensures that the latest revision is loaded into the
        content cache before it is admitted into the revision cache.
        """
        hexsha, modified = self.latest_revision()
        if hexsha not in self._content_cache:
            self._content_cache[hexsha] = self.read_raw(hexsha, modified)
        return hexsha

    def clear_caches(self):
        """Clear the caches."""
        self._revision_cache.clear()
        self._content_cache.clear()


class AsyncCacheableSource(Generic[T], metaclass=ABCMeta):
    """Abstract base class for async sources that can be cached.

    Async equivalent of CacheableSource. Uses AsyncTwoLevelCache so populate
    functions are native coroutines.

    Attributes:
        db_class: Database class associated with the source.
        rev_cache_soft_ttl: Soft time-to-live for revision cache entries.
        rev_cache_hard_ttl: Hard time-to-live for revision cache entries.
        _revision_cache: Asynchronous cache containing latest revision metadata.
        _content_cache: Cache containing source content by revision.
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
        """Return the latest revision and its modification time.

        Returns:
            A tuple containing the revision identifier and its timestamp.
        """

    @abstractmethod
    async def read_raw(self, hexsha: str, modified: datetime) -> T:
        """Fetch the data for a given revision.

        Args:
            hexsha: Identifier of the requested revision.
            modified: Time associated with the requested revision.

        Returns:
            The source data corresponding to the revision.
        """

    async def _read_work(self) -> str:
        hexsha, modified = await self.latest_revision()
        if hexsha not in self._content_cache:
            self._content_cache[hexsha] = await self.read_raw(hexsha, modified)
        return hexsha

    async def read(self) -> T:
        """Perform a blocking read, awaiting refresh on a hard cache miss.

        Returns:
            The latest source data.
        """
        hexsha = await self._revision_cache.get(
            "latest_revision", self._read_work, blocking=True
        )
        return self._content_cache[hexsha]

    async def read_non_blocking(self) -> T:
        """Perform a non-blocking read.

        Returns:
            The latest source data.

        Raises:
            NotReadyError: If the data is not available after a hard cache miss.
        """
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

        Returns:
            Source data from the wired dependency.

        Raises:
            NotImplementedError: If the source was not wired by the factory.
        """
        raise NotImplementedError(f"{cls.__name__} was not wired by the factory")
