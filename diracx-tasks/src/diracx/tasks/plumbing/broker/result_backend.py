from __future__ import annotations

__all__ = ["RedisResultBackend"]

import msgpack
from redis.asyncio import BlockingConnectionPool, Redis

from ..exceptions import ResultIsMissingError
from ._types import _BlockingConnectionPool
from .models import TaskResult

DEFAULT_RESULT_TTL = 86400  # 24 hours


class RedisResultBackend:
    """Result backend storing results in Redis with msgpack serialization."""

    def __init__(
        self,
        redis_url: str,
        prefix: str = "diracx:results",
        result_ttl_seconds: int | None = None,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: object,
    ) -> None:
        self.redis_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
            url=redis_url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,  # type: ignore[arg-type]
        )
        self.prefix = prefix
        self.result_ttl_seconds = result_ttl_seconds or DEFAULT_RESULT_TTL

    def _task_key(self, task_id: str) -> str:
        return f"{self.prefix}:{task_id}"

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        await self.redis_pool.disconnect()

    async def set_result(self, task_id: str, result: TaskResult) -> None:
        async with Redis(connection_pool=self.redis_pool) as redis:
            serialized = msgpack.packb(result.model_dump(), datetime=True)
            await redis.setex(
                name=self._task_key(task_id),
                time=self.result_ttl_seconds,
                value=serialized,
            )

    async def is_result_ready(self, task_id: str) -> bool:
        async with Redis(connection_pool=self.redis_pool) as redis:
            return bool(await redis.exists(self._task_key(task_id)))

    async def get_result(self, task_id: str) -> TaskResult:
        async with Redis(connection_pool=self.redis_pool) as redis:
            result_bytes = await redis.get(name=self._task_key(task_id))

        if result_bytes is None:
            raise ResultIsMissingError(
                f"Result for task {task_id} is missing or has expired"
            )

        result_dict = msgpack.unpackb(result_bytes, timestamp=3)
        return TaskResult.model_validate(result_dict)
