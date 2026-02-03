from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    ForeignKey,
    Index,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from typing_extensions import Annotated

from diracx.db.sql.utils import (
    str32,
    str64,
    str128,
    str255,
)


class TaskQueueDBBase(DeclarativeBase):
    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str255: String(255),
    }


tqid_type = Annotated[
    int,
    mapped_column(
        ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    ),
]
value_type = Annotated[str64, mapped_column(primary_key=True)]


class TaskQueues(TaskQueueDBBase):
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
    __tablename__ = "tq_Jobs"
    TQId: Mapped[tqid_type]
    JobId: Mapped[int] = mapped_column(primary_key=True)
    Priority: Mapped[int]
    RealPriority: Mapped[float]
    __table_args__ = (Index("TaskIndex", "TQId"),)


class SitesQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToSites"
    TQId: Mapped[tqid_type]
    Value: Mapped[value_type]
    __table_args__ = (
        Index("SitesTaskIndex", "TQId"),
        Index("SitesIndex", "Value"),
    )


class GridCEsQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToGridCEs"
    TQId: Mapped[tqid_type]
    Value: Mapped[value_type]
    __table_args__ = (
        Index("GridCEsTaskIndex", "TQId"),
        Index("GridCEsValueIndex", "Value"),
    )


class BannedSitesQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToBannedSites"
    TQId: Mapped[tqid_type]
    Value: Mapped[value_type]
    __table_args__ = (
        Index("BannedSitesTaskIndex", "TQId"),
        Index("BannedSitesValueIndex", "Value"),
    )


class PlatformsQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToPlatforms"
    TQId: Mapped[tqid_type]
    Value: Mapped[value_type]
    __table_args__ = (
        Index("PlatformsTaskIndex", "TQId"),
        Index("PlatformsValueIndex", "Value"),
    )


class JobTypesQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToJobTypes"
    TQId: Mapped[tqid_type]
    Value: Mapped[value_type]
    __table_args__ = (
        Index("JobTypesTaskIndex", "TQId"),
        Index("JobTypesValueIndex", "Value"),
    )


class TagsQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToTags"
    TQId: Mapped[tqid_type]
    Value: Mapped[value_type]
    __table_args__ = (
        Index("TagsTaskIndex", "TQId"),
        Index("TagsValueIndex", "Value"),
    )
