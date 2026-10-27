"""SQL database operations for task dead letter queue persistence."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import (
    delete,
    func,
    insert,
    select,
    update,
)

from diracx.db.sql.task.schema import DeadLetterQueue, DLQBase, DLQStatus
from diracx.db.sql.utils.base import BaseSQLDB
from diracx.db.sql.utils.functions import utcnow

logger = logging.getLogger(__name__)


class TaskDB(BaseSQLDB):
    """Database for task dead letter queue persistence."""

    metadata = DLQBase.metadata

    async def insert_dlq_task(
        self,
        task_class: str,
        task_args: bytes,
        max_retries: int,
        last_error: str | None = None,
    ) -> int:
        """Insert a task into the dead letter queue."""
        stmt = insert(DeadLetterQueue).values(
            task_class=task_class,
            task_args=task_args,
            submitted_at=utcnow(),
            status=DLQStatus.PENDING,
            retry_count=0,
            max_retries=max_retries,
            last_error=last_error,
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

    async def count_dlq_tasks(self) -> dict[tuple[str, str], int]:
        """Number of tasks in the dead letter queue, per task class and status."""
        stmt = select(
            DeadLetterQueue.task_class, DeadLetterQueue.status, func.count()
        ).group_by(DeadLetterQueue.task_class, DeadLetterQueue.status)
        result = await self.conn.execute(stmt)
        return {
            (task_class, str(status)): count for task_class, status, count in result
        }

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
