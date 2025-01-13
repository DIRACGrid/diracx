from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column

TaskQueueDBBase = declarative_base()


class TaskQueues(TaskQueueDBBase):
    __tablename__ = "tq_TaskQueues"
    TQId = Column(Integer, primary_key=True)
    Owner = Column(String(255), nullable=False)
    OwnerGroup = Column(String(32), nullable=False)
    VO = Column(String(32), nullable=False)
    CPUTime = Column(BigInteger, nullable=False)
    Priority = Column(Float, nullable=False)
    Enabled = Column(Boolean, nullable=False, default=0)
    __table_args__ = (Index("TQOwner", "Owner", "OwnerGroup", "CPUTime"),)


class JobsQueue(TaskQueueDBBase):
    __tablename__ = "tq_Jobs"
    TQId = Column(
        Integer, ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    JobId = Column(Integer, primary_key=True)
    Priority = Column(Integer, nullable=False)
    RealPriority = Column(Float, nullable=False)
    __table_args__ = (Index("TaskIndex", "TQId"),)


class SitesQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToSites"
    TQId = Column(
        Integer, ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value = Column(String(64), primary_key=True)
    __table_args__ = (
        Index("SitesTaskIndex", "TQId"),
        Index("SitesIndex", "Value"),
    )


class GridCEsQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToGridCEs"
    TQId = Column(
        Integer, ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value = Column(String(64), primary_key=True)
    __table_args__ = (
        Index("GridCEsTaskIndex", "TQId"),
        Index("GridCEsValueIndex", "Value"),
    )


class BannedSitesQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToBannedSites"
    TQId = Column(
        Integer, ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value = Column(String(64), primary_key=True)
    __table_args__ = (
        Index("BannedSitesTaskIndex", "TQId"),
        Index("BannedSitesValueIndex", "Value"),
    )


class PlatformsQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToPlatforms"
    TQId = Column(
        Integer, ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value = Column(String(64), primary_key=True)
    __table_args__ = (
        Index("PlatformsTaskIndex", "TQId"),
        Index("PlatformsValueIndex", "Value"),
    )


class JobTypesQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToJobTypes"
    TQId = Column(
        Integer, ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value = Column(String(64), primary_key=True)
    __table_args__ = (
        Index("JobTypesTaskIndex", "TQId"),
        Index("JobTypesValueIndex", "Value"),
    )


class TagsQueue(TaskQueueDBBase):
    __tablename__ = "tq_TQToTags"
    TQId = Column(
        Integer, ForeignKey("tq_TaskQueues.TQId", ondelete="CASCADE"), primary_key=True
    )
    Value = Column(String(64), primary_key=True)
    __table_args__ = (
        Index("TagsTaskIndex", "TQId"),
        Index("TagsValueIndex", "Value"),
    )
