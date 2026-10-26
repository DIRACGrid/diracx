"""SQLAlchemy schemas for task dead letter queue persistence."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    DateTime,
    Enum,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class DLQStatus(StrEnum):
    """Status of a task in the dead letter queue."""

    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    FAILED = "FAILED"


class DLQBase(DeclarativeBase):
    """Base class for all dead letter queue tables."""

    pass


class DeadLetterQueue(DLQBase):
    """Table for the dead letter queue."""

    __tablename__ = "dlq_Tasks"

    id: Mapped[int] = mapped_column("ID", Integer, primary_key=True, autoincrement=True)
    task_class: Mapped[str] = mapped_column("TaskClass", String(255), nullable=False)
    task_args: Mapped[bytes] = mapped_column("TaskArgs", LargeBinary, nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(
        "SubmittedAt", DateTime, nullable=False
    )
    status: Mapped[str] = mapped_column(
        "Status", Enum(DLQStatus), default=DLQStatus.PENDING, nullable=False
    )
    retry_count: Mapped[int] = mapped_column("RetryCount", Integer, default=0)
    max_retries: Mapped[int] = mapped_column("MaxRetries", Integer, nullable=False)
    last_error: Mapped[str | None] = mapped_column("LastError", Text, nullable=True)
    last_attempted_at: Mapped[datetime | None] = mapped_column(
        "LastAttemptedAt", DateTime, nullable=True
    )
