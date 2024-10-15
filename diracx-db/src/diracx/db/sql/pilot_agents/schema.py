from sqlalchemy import (
    DateTime,
    Double,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, EnumBackedBool, NullColumn

PilotAgentsDBBase = declarative_base()


class PilotAgents(PilotAgentsDBBase):
    __tablename__ = "PilotAgents"

    PilotID = Column("PilotID", Integer, autoincrement=True, primary_key=True)
    InitialJobID = Column("InitialJobID", Integer, default=0)
    CurrentJobID = Column("CurrentJobID", Integer, default=0)
    PilotJobReference = Column("PilotJobReference", String(255), default="Unknown")
    PilotStamp = Column("PilotStamp", String(32), default="")
    DestinationSite = Column("DestinationSite", String(128), default="NotAssigned")
    Queue = Column("Queue", String(128), default="Unknown")
    GridSite = Column("GridSite", String(128), default="Unknown")
    VO = Column("VO", String(128))
    GridType = Column("GridType", String(32), default="LCG")
    BenchMark = Column("BenchMark", Double, default=0.0)
    SubmissionTime = NullColumn("SubmissionTime", DateTime)
    LastUpdateTime = NullColumn("LastUpdateTime", DateTime)
    Status = Column("Status", String(32), default="Unknown")
    StatusReason = Column("StatusReason", String(255), default="Unknown")
    AccountingSent = Column("AccountingSent", EnumBackedBool(), default=False)

    __table_args__ = (
        Index("PilotJobReference", "PilotJobReference"),
        Index("Status", "Status"),
        Index("Statuskey", "GridSite", "DestinationSite", "Status"),
    )


class JobToPilotMapping(PilotAgentsDBBase):
    __tablename__ = "JobToPilotMapping"

    PilotID = Column("PilotID", Integer, primary_key=True)
    JobID = Column("JobID", Integer, primary_key=True)
    StartTime = Column("StartTime", DateTime)

    __table_args__ = (Index("JobID", "JobID"), Index("PilotID", "PilotID"))


class PilotOutput(PilotAgentsDBBase):
    __tablename__ = "PilotOutput"

    PilotID = Column("PilotID", Integer, primary_key=True)
    StdOutput = Column("StdOutput", Text)
    StdError = Column("StdError", Text)
