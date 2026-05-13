from __future__ import annotations

__all__ = [
    "BaseLimiter",
    "BaseLock",
    "ConcurrencyLimiter",
    "ExclusiveRWLock",
    "MutexLock",
    "RateLimiter",
    "SharedRWLock",
]

import logging
import time
import uuid
from abc import ABC, abstractmethod

from ._redis_types import LockCoordinator
from .lock_registry import LockedObjectType

logger = logging.getLogger(__name__)

# Lua script for extending a mutex lock only if the owner matches
_MUTEX_EXTEND_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Lua script for releasing a mutex lock only if the owner matches
_MUTEX_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Lua script for acquiring a shared read lock (if no writer holds exclusive)
_SHARED_ACQUIRE_SCRIPT = """
local writers = redis.call("hget", KEYS[1], "writer_owner")
if writers then
    return 0
end
redis.call("hincrby", KEYS[1], "readers", 1)
return 1
"""

# Lua script for releasing a shared read lock
_SHARED_RELEASE_SCRIPT = """
local readers = redis.call("hincrby", KEYS[1], "readers", -1)
if readers <= 0 then
    redis.call("hdel", KEYS[1], "readers")
end
return readers
"""

# Lua script for acquiring an exclusive write lock
_EXCLUSIVE_ACQUIRE_SCRIPT = """
local readers = tonumber(redis.call("hget", KEYS[1], "readers") or "0")
local writer = redis.call("hget", KEYS[1], "writer_owner")
if readers == 0 and not writer then
    redis.call("hset", KEYS[1], "writer_owner", ARGV[1])
    redis.call("pexpire", KEYS[1], ARGV[2])
    return 1
end
return 0
"""

# Lua script for extending an exclusive write lock only if the owner matches
_EXCLUSIVE_EXTEND_SCRIPT = """
if redis.call("hget", KEYS[1], "writer_owner") == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Lua script for releasing an exclusive write lock
_EXCLUSIVE_RELEASE_SCRIPT = """
if redis.call("hget", KEYS[1], "writer_owner") == ARGV[1] then
    redis.call("hdel", KEYS[1], "writer_owner")
    return 1
end
return 0
"""

# Lua script for atomic rate-limit check-and-increment
_RATE_LIMIT_SCRIPT = """
local current = tonumber(redis.call("get", KEYS[1]) or "0")
if current + tonumber(ARGV[1]) > tonumber(ARGV[2]) then
    return 0
end
redis.call("incrby", KEYS[1], ARGV[1])
redis.call("expire", KEYS[1], ARGV[3])
return 1
"""

# Lua script for concurrency limiter acquire (ZSET-based).
# Prunes expired members, then adds the new owner if under the limit.
_CONCURRENCY_ACQUIRE_SCRIPT = """
redis.call("zremrangebyscore", KEYS[1], "-inf", ARGV[1])
local current = redis.call("zcard", KEYS[1])
if current < tonumber(ARGV[2]) then
    redis.call("zadd", KEYS[1], ARGV[3], ARGV[4])
    return 1
end
return 0
"""

# Lua script for concurrency limiter release (ZSET-based).
_CONCURRENCY_RELEASE_SCRIPT = """
return redis.call("zrem", KEYS[1], ARGV[1])
"""

DEFAULT_LOCK_TTL_MS = 30000  # 30 seconds


class BaseLock(ABC):
    """Base class for all Redis-backed lock primitives.

    Each lock is scoped to a ``(LockedObjectType, key, *extra_keys)``
    tuple which is joined into a Redis key.  A random ``_owner_id``
    is generated per instance so that release operations can verify
    ownership (preventing one worker from releasing another's lock).

    Subclasses implement ``acquire`` / ``release`` using Redis commands
    or Lua scripts to ensure atomicity.
    """

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
    ):
        self.obj = obj
        self.key = key
        self.extra_keys = extra_keys
        self._owner_id = uuid.uuid4().hex

    @property
    def redis_key(self) -> str:
        """Compute the Redis key for this lock."""
        parts = [str(self.obj), str(self.key)]
        parts.extend(str(k) for k in self.extra_keys)
        return ":".join(parts)

    @abstractmethod
    async def acquire(self, redis: LockCoordinator) -> bool:
        """Attempt to acquire the lock.

        Returns True if acquired, False otherwise.
        """
        ...

    @abstractmethod
    async def release(self, redis: LockCoordinator) -> None:
        """Release the lock."""
        ...

    async def extend(self, redis: LockCoordinator) -> bool:
        """Extend the TTL of the lock (watchdog pattern).

        Returns True if the extension succeeded, False if the lock
        is not held or the primitive does not support extension.
        Subclasses with TTLs should override this.
        """
        return False


class MutexLock(BaseLock):
    """Mutual-exclusion lock using ``SET key owner NX PX ttl``.

    At most one owner can hold the lock at a time.  Acquire is a
    single atomic ``SET NX``; release uses a Lua script that deletes
    the key only if the stored value matches ``_owner_id``, preventing
    accidental release by a different worker.  The TTL acts as a
    safety net — if the holder crashes, the lock auto-expires.

    Use ``extend()`` to refresh the TTL for long-running tasks
    (watchdog pattern).
    """

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
        ttl_ms: int = DEFAULT_LOCK_TTL_MS,
    ):
        super().__init__(obj, key, *extra_keys)
        self.ttl_ms = ttl_ms

    @property
    def redis_key(self) -> str:
        return f"lock:mutex:{super().redis_key}"

    async def acquire(self, redis: LockCoordinator) -> bool:
        result = await redis.set(
            self.redis_key,
            self._owner_id,
            nx=True,
            px=self.ttl_ms,
        )
        return result is not None

    async def release(self, redis: LockCoordinator) -> None:
        await redis.eval(  # type: ignore[arg-type]
            _MUTEX_RELEASE_SCRIPT, 1, self.redis_key, self._owner_id
        )

    async def extend(self, redis: LockCoordinator) -> bool:
        """Extend the TTL only if we still own the lock."""
        result = await redis.eval(  # type: ignore[arg-type]
            _MUTEX_EXTEND_SCRIPT, 1, self.redis_key, self._owner_id, str(self.ttl_ms)
        )
        return bool(result)


class ExclusiveRWLock(BaseLock):
    """Exclusive (writer) side of a reader-writer lock.

    Uses a Redis hash with ``readers`` (int) and ``writer_owner``
    (string) fields.  Acquire succeeds only when there are zero
    readers and no existing writer, set atomically via Lua script.
    The hash gets a TTL so it auto-expires if the writer crashes.

    Pair with ``SharedRWLock`` on the same ``(obj, key)`` to allow
    concurrent readers or a single exclusive writer.
    """

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
        ttl_ms: int = DEFAULT_LOCK_TTL_MS,
    ):
        super().__init__(obj, key, *extra_keys)
        self.ttl_ms = ttl_ms

    @property
    def redis_key(self) -> str:
        return f"lock:rw:{super().redis_key}"

    async def acquire(self, redis: LockCoordinator) -> bool:
        result = await redis.eval(  # type: ignore[arg-type]
            _EXCLUSIVE_ACQUIRE_SCRIPT,
            1,
            self.redis_key,
            self._owner_id,
            str(self.ttl_ms),
        )
        return bool(result)

    async def release(self, redis: LockCoordinator) -> None:
        await redis.eval(  # type: ignore[arg-type]
            _EXCLUSIVE_RELEASE_SCRIPT, 1, self.redis_key, self._owner_id
        )

    async def extend(self, redis: LockCoordinator) -> bool:
        """Extend the TTL only if we still own the write lock."""
        result = await redis.eval(  # type: ignore[arg-type]
            _EXCLUSIVE_EXTEND_SCRIPT,
            1,
            self.redis_key,
            self._owner_id,
            str(self.ttl_ms),
        )
        return bool(result)


class SharedRWLock(BaseLock):
    """Shared (reader) side of a reader-writer lock.

    Multiple readers can hold the lock concurrently.  Acquire
    succeeds as long as no writer holds the exclusive side (checked
    atomically via Lua script by looking for a ``writer_owner``
    field in the hash).  Release decrements the ``readers`` counter
    and cleans it up when it reaches zero.

    Pair with ``ExclusiveRWLock`` on the same ``(obj, key)``.
    """

    @property
    def redis_key(self) -> str:
        return f"lock:rw:{super().redis_key}"

    async def acquire(self, redis: LockCoordinator) -> bool:
        result = await redis.eval(  # type: ignore[arg-type]
            _SHARED_ACQUIRE_SCRIPT, 1, self.redis_key
        )
        return bool(result)

    async def release(self, redis: LockCoordinator) -> None:
        await redis.eval(  # type: ignore[arg-type]
            _SHARED_RELEASE_SCRIPT, 1, self.redis_key
        )


class BaseLimiter(BaseLock):
    """Base class for limiters.

    Limiters are lock primitives that are only enforced for non-interactive
    task execution (skipped in CLI/interactive mode).
    """


class RateLimiter(BaseLimiter):
    """Sliding-window rate limiter.

    Limits the number of operations within a fixed time window.
    The window key is ``{redis_key}:{epoch // window_seconds}``,
    so each window naturally expires.  Check-and-increment is
    performed atomically via a Lua script to prevent concurrent
    workers from exceeding the limit.

    Set ``limit`` and ``window_seconds`` as class variables on
    subclasses.  When either is ``None``, acquire always succeeds
    (limiter is disabled).  ``release`` is a no-op — consumed
    quota is not returned.
    """

    limit: int | None = None
    window_seconds: int | None = None

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
        n_items: int = 1,
    ):
        super().__init__(obj, key, *extra_keys)
        self.n_items = n_items

    @property
    def redis_key(self) -> str:
        return f"limiter:rate:{super().redis_key}"

    async def acquire(self, redis: LockCoordinator) -> bool:
        if self.limit is None or self.window_seconds is None:
            return True

        window_key = f"{self.redis_key}:{int(time.time()) // self.window_seconds}"
        result = await redis.eval(  # type: ignore[arg-type]
            _RATE_LIMIT_SCRIPT,
            1,
            window_key,
            str(self.n_items),
            str(self.limit),
            str(self.window_seconds * 2),
        )
        return bool(result)

    async def release(self, redis: LockCoordinator) -> None:
        pass  # Rate limiters don't release


class ConcurrencyLimiter(BaseLimiter):
    """Semaphore-style concurrency limiter backed by a Redis sorted set.

    Caps the number of workers that can execute a given task
    simultaneously.  Each holder is tracked as a ZSET member
    (keyed by ``_owner_id``) with its expiry timestamp as the
    score.  On every acquire, expired members are pruned first,
    so crashed workers' slots are automatically reclaimed — no
    heartbeat or background cleanup required.

    Set ``limit`` as a class variable on subclasses.  When
    ``limit`` is ``None``, the limiter is disabled.

    Use ``extend()`` to push back the expiry for long-running
    tasks (watchdog pattern).
    """

    limit: int | None = None

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
        ttl_ms: int = DEFAULT_LOCK_TTL_MS,
    ):
        super().__init__(obj, key, *extra_keys)
        self.ttl_ms = ttl_ms

    @property
    def redis_key(self) -> str:
        return f"limiter:conc:{super().redis_key}"

    async def acquire(self, redis: LockCoordinator) -> bool:
        if self.limit is None:
            return True

        now_ms = int(time.time() * 1000)
        expiry_ms = now_ms + self.ttl_ms
        result = await redis.eval(  # type: ignore[arg-type]
            _CONCURRENCY_ACQUIRE_SCRIPT,
            1,
            self.redis_key,
            str(now_ms),
            str(self.limit),
            str(expiry_ms),
            self._owner_id,
        )
        return bool(result)

    async def release(self, redis: LockCoordinator) -> None:
        if self.limit is None:
            return
        await redis.eval(  # type: ignore[arg-type]
            _CONCURRENCY_RELEASE_SCRIPT, 1, self.redis_key, self._owner_id
        )

    async def extend(self, redis: LockCoordinator) -> bool:
        """Extend the expiry for this holder (watchdog pattern)."""
        expiry_ms = int(time.time() * 1000) + self.ttl_ms
        result = await redis.zadd(self.redis_key, {self._owner_id: expiry_ms}, xx=True)
        return bool(result)
