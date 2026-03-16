from __future__ import annotations

__all__ = ["TaskDB"]

import logging
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    Integer,
    LargeBinary,
    String,
    Text,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils.base import BaseSQLDB
from diracx.db.sql.utils.functions import utcnow

logger = logging.getLogger(__name__)


class DLQStatus(StrEnum):
    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    FAILED = "FAILED"


class DLQBase(DeclarativeBase):
    pass


class DeadLetterQueue(DLQBase):
    __tablename__ = "dlq_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_class: Mapped[str] = mapped_column(String(255), nullable=False)
    task_args: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(
        Enum(DLQStatus), default=DLQStatus.PENDING, nullable=False
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_attempted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TaskDB(BaseSQLDB):
    """Database for task dead letter queue persistence."""

    metadata = DLQBase.metadata

    async def insert_dlq_task(
        self,
        task_class: str,
        task_args: bytes,
        max_retries: int,
    ) -> int:
        """Insert a task into the dead letter queue."""
        stmt = insert(DeadLetterQueue).values(
            task_class=task_class,
            task_args=task_args,
            submitted_at=utcnow(),
            status=DLQStatus.PENDING,
            retry_count=0,
            max_retries=max_retries,
        )
        result = await self.conn.execute(stmt)
        return result.lastrowid

    async def mark_dispatched(self, dlq_id: int) -> None:
        """Mark a dead letter queue task as dispatched to Redis."""
        stmt = (
            update(DeadLetterQueue)
            .where(DeadLetterQueue.id == dlq_id)
            .values(status=DLQStatus.DISPATCHED)
        )
        await self.conn.execute(stmt)

    async def mark_failed(self, dlq_id: int, error: str) -> None:
        """Mark a dead letter queue task as permanently failed."""
        stmt = (
            update(DeadLetterQueue)
            .where(DeadLetterQueue.id == dlq_id)
            .values(
                status=DLQStatus.FAILED,
                last_error=error,
                last_attempted_at=utcnow(),
            )
        )
        await self.conn.execute(stmt)

    async def delete_dlq_task(self, dlq_id: int) -> None:
        """Remove a completed task from the dead letter queue."""
        stmt = delete(DeadLetterQueue).where(DeadLetterQueue.id == dlq_id)
        await self.conn.execute(stmt)

    async def get_pending_tasks(self, batch_size: int = 100) -> list[dict[str, Any]]:
        """Get PENDING/DISPATCHED tasks for re-submission on broker startup."""
        stmt = (
            select(DeadLetterQueue)
            .where(
                DeadLetterQueue.status.in_([DLQStatus.PENDING, DLQStatus.DISPATCHED])
            )
            .limit(batch_size)
        )
        result = await self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
