from __future__ import annotations

from typing import TYPE_CHECKING

from redis.asyncio import BlockingConnectionPool, Connection

if TYPE_CHECKING:
    from typing import TypeAlias

    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool[Connection]  # type: ignore[type-arg]
else:
    from typing import TypeAlias

    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool
