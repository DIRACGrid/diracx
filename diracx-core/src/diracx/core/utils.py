from __future__ import annotations

__all__ = [
    "dotenv_files_from_environment",
    "serialize_credentials",
    "read_credentials",
    "write_credentials",
    "TwoLevelCache",
]

import fcntl
import json
import os
import re
import threading
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TypeVar

from cachetools import Cache, TTLCache

from diracx.core.exceptions import NotReadyError
from diracx.core.models import TokenResponse

EXPIRES_GRACE_SECONDS = 15

T = TypeVar("T")


def dotenv_files_from_environment(prefix: str) -> list[str]:
    """Get the sorted list of .env files to use for configuration."""
    env_files = {}
    for key, value in os.environ.items():
        if match := re.fullmatch(rf"{prefix}(?:_(\d+))?", key):
            env_files[int(match.group(1) or -1)] = value
    return [v for _, v in sorted(env_files.items())]


def serialize_credentials(token_response: TokenResponse) -> str:
    """Serialize DiracX client credentials to a string.

    This method is separated from write_credentials to allow for DIRAC to be
    able to serialize credentials for inclusion in the proxy file.
    """
    expires = datetime.now(tz=timezone.utc) + timedelta(
        seconds=token_response.expires_in - EXPIRES_GRACE_SECONDS
    )
    credential_data = {
        "access_token": token_response.access_token,
        "refresh_token": token_response.refresh_token,
        "expires_on": int(datetime.timestamp(expires)),
    }
    return json.dumps(credential_data)


def read_credentials(location: Path) -> TokenResponse:
    """Read credentials from a file."""
    from diracx.core.preferences import get_diracx_preferences

    credentials_path = location or get_diracx_preferences().credentials_path
    try:
        with open(credentials_path, "r") as f:
            # Lock the file to prevent other processes from writing to it at the same time
            fcntl.flock(f, fcntl.LOCK_SH)
            # Read the credentials from the file
            try:
                credentials = json.load(f)
            finally:
                # Release the lock
                fcntl.flock(f, fcntl.LOCK_UN)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Error reading credentials: {e}") from e

    return TokenResponse(
        access_token=credentials["access_token"],
        expires_in=credentials["expires_on"]
        - int(datetime.now(tz=timezone.utc).timestamp()),
        token_type="Bearer",  # noqa: S106
        refresh_token=credentials.get("refresh_token"),
    )


def write_credentials(token_response: TokenResponse, *, location: Path | None = None):
    """Write credentials received in dirax_preferences.credentials_path."""
    from diracx.core.preferences import get_diracx_preferences

    credentials_path = location or get_diracx_preferences().credentials_path
    credentials_path.parent.mkdir(parents=True, exist_ok=True)

    with open(credentials_path, "w") as f:
        # Lock the file to prevent other processes from writing to it at the same time
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            # Write the credentials to the file
            f.write(serialize_credentials(token_response))
            f.flush()
            os.fsync(f.fileno())
        finally:
            # Release the lock
            fcntl.flock(f, fcntl.LOCK_UN)


class TwoLevelCache:
    """A two-level caching system with soft and hard time-to-live (TTL) expiration.

    This cache implements a two-tier caching mechanism to allow for background refresh
    of cached values. It uses a soft TTL for quick access and a hard TTL as a fallback,
    which helps in reducing latency and maintaining data freshness.

    Attributes:
        soft_cache (TTLCache): A cache with a shorter TTL for quick access.
        hard_cache (TTLCache): A cache with a longer TTL as a fallback.
        locks (defaultdict): Thread-safe locks for each cache key.
        futures (dict): Stores ongoing asynchronous population tasks.
        pool (ThreadPoolExecutor): Thread pool for executing cache population tasks.

    Args:
        soft_ttl (int): Time-to-live in seconds for the soft cache.
        hard_ttl (int): Time-to-live in seconds for the hard cache.
        max_workers (int): Maximum number of workers in the thread pool.
        max_items (int): Maximum number of items in the cache.

    Example:
        >>> cache = TwoLevelCache(soft_ttl=60, hard_ttl=300)
        >>> def populate_func():
        ...     return "cached_value"
        >>> value = cache.get("key", populate_func)

    """

    def __init__(
        self,
        soft_ttl: int,
        hard_ttl: int,
        *,
        max_workers: int = 10,
        max_items: int = 1_000_000,
    ):
        """Initialize the TwoLevelCache with specified TTLs."""
        self.soft_cache: Cache = TTLCache(max_items, soft_ttl)
        self.hard_cache: Cache = TTLCache(max_items, hard_ttl)
        self.locks: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)
        self.futures: dict[str, Future] = {}
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def get(self, key: str, populate_func: Callable[[], T], blocking: bool = True) -> T:
        """Retrieve a value from the cache, populating it if necessary.

        This method first checks the soft cache for the key. If not found,
        it checks the hard cache while initiating a background refresh.
        If the key is not in either cache, it waits for the populate_func
        to complete and stores the result in both caches.

        Locks are used to ensure there is never more than one concurrent
        population task for a given key.

        Args:
            key (str): The cache key to retrieve or populate.
            populate_func (Callable[[], Any]): A function to call to populate the cache
                                               if the key is not found.

            blocking (bool): If True, wait for the cache to be populated if the key is not
                            found. If False, raise NotReadyError if the key is not ready.

        Returns:
            Any: The cached value associated with the key.

        Note:
            This method is thread-safe and handles concurrent requests for the same key.

        """
        if result := self.soft_cache.get(key):
            return result
        if self.locks[key].acquire(blocking=blocking):
            try:
                if key not in self.futures:
                    self.futures[key] = self.pool.submit(self._work, key, populate_func)
                if result := self.hard_cache.get(key):
                    # The soft cache will be updated by _work so we can fill the soft
                    # cache to avoid later requests needign to acquire the lock.
                    self.soft_cache[key] = result
                    return result
                future = self.futures[key]
            finally:
                self.locks[key].release()
            if blocking:
                # It is critical that ``future`` is waited for outside of the lock
                # as _work aquires the lock before filling the caches. This also
                # means we can guarantee that the future has not yet been removed
                # from the futures dict.
                wait([future])
                return self.hard_cache[key]

        # If the lock is not acquired we're in a non-blocking mode, try to get the
        # value from the hard cache. If it's not there, raise NotReadyError.
        if result := self.hard_cache.get(key):
            return result
        raise NotReadyError(f"Cache key {key} is not ready yet.")

    def _work(self, key: str, populate_func: Callable[[], Any]) -> None:
        """Internal method to execute the populate_func and update caches.

        This method is intended to be run in a separate thread. It calls the
        populate_func, stores the result in both caches, and cleans up the
        associated future.

        Args:
            key (str): The cache key to populate.
            populate_func (Callable[[], Any]): The function to call to get the value.

        Note:
            This method is not intended to be called directly by users of the class.

        """
        result = populate_func()
        with self.locks[key]:
            self.futures.pop(key)
            self.hard_cache[key] = result
            self.soft_cache[key] = result

    def clear(self):
        """Clear all caches and reset the thread pool."""
        self.pool.shutdown(wait=True)
        self.pool = ThreadPoolExecutor(max_workers=self.pool._max_workers)
        self.soft_cache.clear()
        self.hard_cache.clear()
        self.futures.clear()
        self.locks.clear()
