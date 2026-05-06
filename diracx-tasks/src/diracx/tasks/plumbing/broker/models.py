from __future__ import annotations

__all__ = [
    "TaskMessage",
    "TaskResult",
    "ReceivedMessage",
    "TaskBinding",
    "submit_task",
]

import dataclasses
import logging
import traceback
from collections.abc import Sequence
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Generic, TypeVar

import msgpack
from pydantic import BaseModel, ConfigDict, Field

from ..exceptions import SendTaskError

if TYPE_CHECKING:
    from .redis_streams import RedisStreamBroker

logger = logging.getLogger(__name__)

_ReturnType = TypeVar("_ReturnType")


class TaskMessage(BaseModel):
    """Wire-protocol message for the task system.

    Serialized directly to msgpack for the broker — no intermediate wrapper.
    """

    task_id: str
    task_name: str
    labels: dict[str, Any]
    task_args: list[Any]
    task_kwargs: dict[str, Any]

    def dumpb(self) -> bytes:
        return msgpack.packb(self.model_dump(), datetime=True)

    @classmethod
    def loadb(cls, data: bytes) -> TaskMessage:
        return cls.model_validate(msgpack.unpackb(data, timestamp=3))


class TaskResult(BaseModel, Generic[_ReturnType]):
    """Result of task execution."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    is_err: bool
    return_value: _ReturnType
    execution_time: float
    error: dict[str, str] | None = None
    labels: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        execution_time: float,
        labels: dict[str, Any] | None = None,
    ) -> TaskResult[None]:
        return TaskResult[None](
            is_err=True,
            return_value=None,
            execution_time=execution_time,
            error={
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
            },
            labels=labels or {},
        )

    @classmethod
    def from_value(
        cls,
        value: _ReturnType,
        execution_time: float,
        labels: dict[str, Any] | None = None,
    ) -> TaskResult[_ReturnType]:
        return cls(
            is_err=False,
            return_value=value,
            execution_time=execution_time,
            error=None,
            labels=labels or {},
        )

    def raise_for_error(self) -> TaskResult[_ReturnType]:
        if self.is_err and self.error:
            raise Exception(f"[{self.error['type']}] {self.error['message']}")
        return self


class ReceivedMessage(BaseModel):
    """Message that can be acknowledged after processing."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: bytes
    ack: Callable[[], Awaitable[None]]
    renew: Callable[[], Awaitable[None]]


def _prepare_arg(arg: Any) -> Any:
    """Convert BaseModel/dataclass arguments to dicts for serialization."""
    if isinstance(arg, BaseModel):
        arg = arg.model_dump()
    if is_dataclass(arg) and not isinstance(arg, type):
        arg = asdict(arg)
    return arg


def _build_task_message(
    broker: RedisStreamBroker,
    task_name: str,
    task_args: Sequence[Any],
    task_kwargs: dict[str, Any],
    labels: dict[str, Any],
    task_id: str | None,
) -> TaskMessage:
    """Build a TaskMessage from the given arguments."""
    formatted_args = [_prepare_arg(arg) for arg in task_args]
    formatted_kwargs = {k: _prepare_arg(v) for k, v in task_kwargs.items()}

    if task_id is None:
        task_id = broker.id_generator()

    return TaskMessage(
        task_id=task_id,
        task_name=task_name,
        labels=labels,
        task_args=formatted_args,
        task_kwargs=formatted_kwargs,
    )


async def submit_task(
    broker: RedisStreamBroker,
    task_name: str,
    task_args: Sequence[Any] = (),
    task_kwargs: dict[str, Any] | None = None,
    *,
    labels: dict[str, Any] | None = None,
    task_id: str | None = None,
    run_at: datetime | None = None,
) -> str:
    """Send a task to the broker for execution.

    When ``run_at`` is provided, the task is added to the delayed ZSET
    and will be promoted to a stream when the time arrives.

    Returns the task_id.
    """
    task_message = _build_task_message(
        broker=broker,
        task_name=task_name,
        task_args=task_args,
        task_kwargs=task_kwargs or {},
        labels=labels or {},
        task_id=task_id,
    )

    if run_at is not None:
        from redis.asyncio import Redis

        from ..scheduler.scheduler import schedule_delayed

        try:
            redis = Redis(connection_pool=broker.connection_pool)
            async with redis:
                await schedule_delayed(redis, task_message, run_at)
        except Exception as exc:
            raise SendTaskError(f"Failed to schedule delayed task {task_name}") from exc
    else:
        try:
            await broker.enqueue(task_message)
        except Exception as exc:
            raise SendTaskError(f"Failed to send task {task_name} to broker") from exc

    return task_message.task_id


@dataclasses.dataclass
class TaskBinding:
    """Maps a task class to its broker for submission."""

    broker: RedisStreamBroker
    task_name: str
    labels: dict[str, Any] = dataclasses.field(default_factory=dict)

    async def submit(
        self,
        *args: Any,
        labels: dict[str, Any] | None = None,
        run_at: datetime | None = None,
        **kwargs: Any,
    ) -> str:
        merged_labels = {**self.labels}
        if labels:
            merged_labels.update(labels)
        return await submit_task(
            broker=self.broker,
            task_name=self.task_name,
            task_args=args,
            task_kwargs=kwargs or None,
            labels=merged_labels,
            run_at=run_at,
        )
