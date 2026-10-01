"""SQLAlchemy schemas for task queues and their associations."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    ForeignKey,
    Index,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils import (
    str32,
    str64,
    str128,
    str255,
)


class TaskQueueDBBase(DeclarativeBase):
    """Declarative base mapping string aliases to SQL column types.

    Attributes:
        type_annotation_map: SQL types associated with the string aliases.
    """

    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str255: String(255),
    }


class TaskQueues(TaskQueueDBBase):
    """Task queue definitions and ownership information.

    Attributes:
        TQId: Task queue identifier.
        Owner: User who owns the task queue.
        OwnerGroup: Group of the task queue owner.
        VO: Virtual organization associated with the queue.
        CPUTime: CPU time requirement.
        Priority: Task queue priority.
        Enabled: Whether the task queue is enabled.
    """

    __tablename__ = "tq_TaskQueues"
    TQId: Mapped[int] = mapped_column(primary_key=True)
    Owner: Mapped[str255]
    OwnerGroup: Mapped[str32]
    VO: Mapped[str32]
    CPUTime: Mapped[int] = mapped_column(BigInteger)
    Priority: Mapped[float]
    Enabled: Mapped[bool] = mapped_column(default=0)
    __table_args__ = (Index("TQOwner", "Owner", "OwnerGroup", "CPUTime"),)


class JobsQueue(TaskQueueDBBase):
    """Job assignments and priorities for task queues.

    Attributes:
        TQId: Identifier of the task queue.
        JobId: Identifier of the assigned job.
        Priority: Job priority within the task queue.
        RealPriority: Effective job priority.
    """

    __tablename__ = "tq_Jobs"
    TQId: Mapped[int] = mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    JobId: Mapped[int] = mapped_column(primary_key=True)
    Priority: Mapped[int]
    RealPriority: Mapped[float]
    __table_args__ = (Index("TaskIndex", "TQId"),)


class SitesQueue(TaskQueueDBBase):
    """Site values associated with task queues.

    Attributes:
        TQId: Identifier of the task queue.
        Value: Site name associated with the queue.
    """

    __tablename__ = "tq_TQToSites"
    TQId: Mapped[int] = mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value: Mapped[str64] = mapped_column(primary_key=True)
    __table_args__ = (
        Index("SitesTaskIndex", "TQId"),
        Index("SitesIndex", "Value"),
    )


class GridCEsQueue(TaskQueueDBBase):
    """Grid computing elements associated with task queues.

    Attributes:
        TQId: Identifier of the task queue.
        Value: Grid computing element associated with the queue.
    """

    __tablename__ = "tq_TQToGridCEs"
    TQId: Mapped[int] = mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value: Mapped[str64] = mapped_column(primary_key=True)
    __table_args__ = (
        Index("GridCEsTaskIndex", "TQId"),
        Index("GridCEsValueIndex", "Value"),
    )


class BannedSitesQueue(TaskQueueDBBase):
    """Sites excluded from task queues.

    Attributes:
        TQId: Identifier of the task queue.
        Value: Banned site name.
    """

    __tablename__ = "tq_TQToBannedSites"
    TQId: Mapped[int] = mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value: Mapped[str64] = mapped_column(primary_key=True)
    __table_args__ = (
        Index("BannedSitesTaskIndex", "TQId"),
        Index("BannedSitesValueIndex", "Value"),
    )


class PlatformsQueue(TaskQueueDBBase):
    """Platforms supported by task queues.

    Attributes:
        TQId: Identifier of the task queue.
        Value: Platform associated with the queue.
    """

    __tablename__ = "tq_TQToPlatforms"
    TQId: Mapped[int] = mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value: Mapped[str64] = mapped_column(primary_key=True)
    __table_args__ = (
        Index("PlatformsTaskIndex", "TQId"),
        Index("PlatformsValueIndex", "Value"),
    )


class JobTypesQueue(TaskQueueDBBase):
    """Job types supported by task queues.

    Attributes:
        TQId: Identifier of the task queue.
        Value: Job type associated with the queue.
    """

    __tablename__ = "tq_TQToJobTypes"
    TQId: Mapped[int] = mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value: Mapped[str64] = mapped_column(primary_key=True)
    __table_args__ = (
        Index("JobTypesTaskIndex", "TQId"),
        Index("JobTypesValueIndex", "Value"),
    )


class TagsQueue(TaskQueueDBBase):
    """Tags associated with task queues.

    Attributes:
        TQId: Identifier of the task queue.
        Value: Tag associated with the queue.
    """

    __tablename__ = "tq_TQToTags"
    TQId: Mapped[int] = mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value: Mapped[str64] = mapped_column(primary_key=True)
    __table_args__ = (
        Index("TagsTaskIndex", "TQId"),
        Index("TagsValueIndex", "Value"),
    )
