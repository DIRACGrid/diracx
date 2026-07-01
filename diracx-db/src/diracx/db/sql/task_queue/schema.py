"""Task queue schema definitions.

Defines the SQLAlchemy ORM mappings for task queues and related queue
membership tables.
"""

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
    """Base declarative class for task queue schema tables.

    The :attr:`type_annotation_map` maps compact string aliases like ``str32``
    to concrete SQLAlchemy ``String`` column types.
    """

    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str255: String(255),
    }


class TaskQueues(TaskQueueDBBase):
    """ORM mapping for task queue definitions.

    Attributes:
        TQId (int): Task queue identifier.
        Owner (str): Task queue owner.
        OwnerGroup (str): Owner group name.
        VO (str): Virtual organization.
        CPUTime (int): Allocated CPU time.
        Priority (float): Task queue priority.
        Enabled (bool): Whether the queue is enabled.
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
    """ORM mapping for jobs assigned to task queues.

    Attributes:
        TQId (int): Task queue identifier.
        JobId (int): Job identifier.
        Priority (int): Assigned job priority.
        RealPriority (float): Real priority value used for scheduling.
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
    """ORM mapping for task queue site restrictions.

    Attributes:
        TQId (int): Task queue identifier.
        Value (str): Site name.
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
    """ORM mapping for task queue CE restrictions.

    Attributes:
        TQId (int): Task queue identifier.
        Value (str): Grid CE name.
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
    """ORM mapping for banned sites associated with task queues.

    Attributes:
        TQId (int): Task queue identifier.
        Value (str): Banned site name.
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
    """ORM mapping for platform restrictions in task queues.

    Attributes:
        TQId (int): Task queue identifier.
        Value (str): Platform name.
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
    """ORM mapping for job type restrictions in task queues.

    Attributes:
        TQId (int): Task queue identifier.
        Value (str): Job type name.
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
    """ORM mapping for tag restrictions in task queues.

    Attributes:
        TQId (int): Task queue identifier.
        Value (str): Tag value.
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
