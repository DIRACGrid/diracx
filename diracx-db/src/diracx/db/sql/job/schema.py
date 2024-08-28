import sqlalchemy.types as types
from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, NullColumn

JobDBBase = declarative_base()


class EnumBackedBool(types.TypeDecorator):
    """Maps a ``EnumBackedBool()`` column to True/False in Python."""

    impl = types.Enum
    cache_ok: bool = True

    def __init__(self) -> None:
        super().__init__("True", "False")

    def process_bind_param(self, value, dialect) -> str:
        if value is True:
            return "True"
        elif value is False:
            return "False"
        else:
            raise NotImplementedError(value, dialect)

    def process_result_value(self, value, dialect) -> bool:
        if value == "True":
            return True
        elif value == "False":
            return False
        else:
            raise NotImplementedError(f"Unknown {value=}")


class Jobs(JobDBBase):
    __tablename__ = "Jobs"

    JobID = Column(
        "JobID",
        Integer,
        ForeignKey("JobJDLs.JobID", ondelete="CASCADE"),
        primary_key=True,
        default=0,
    )
    JobType = Column("JobType", String(32), default="user")
    JobGroup = Column("JobGroup", String(32), default="00000000")
    Site = Column("Site", String(100), default="ANY")
    JobName = Column("JobName", String(128), default="Unknown")
    Owner = Column("Owner", String(64), default="Unknown")
    OwnerGroup = Column("OwnerGroup", String(128), default="Unknown")
    VO = Column("VO", String(32))
    SubmissionTime = NullColumn("SubmissionTime", DateTime)
    RescheduleTime = NullColumn("RescheduleTime", DateTime)
    LastUpdateTime = NullColumn("LastUpdateTime", DateTime)
    StartExecTime = NullColumn("StartExecTime", DateTime)
    HeartBeatTime = NullColumn("HeartBeatTime", DateTime)
    EndExecTime = NullColumn("EndExecTime", DateTime)
    Status = Column("Status", String(32), default="Received")
    MinorStatus = Column("MinorStatus", String(128), default="Unknown")
    ApplicationStatus = Column("ApplicationStatus", String(255), default="Unknown")
    UserPriority = Column("UserPriority", Integer, default=0)
    RescheduleCounter = Column("RescheduleCounter", Integer, default=0)
    VerifiedFlag = Column("VerifiedFlag", EnumBackedBool(), default=False)
    # TODO: Should this be True/False/"Failed"? Or True/False/Null?
    AccountedFlag = Column(
        "AccountedFlag", Enum("True", "False", "Failed"), default="False"
    )

    __table_args__ = (
        Index("JobType", "JobType"),
        Index("JobGroup", "JobGroup"),
        Index("Site", "Site"),
        Index("Owner", "Owner"),
        Index("OwnerGroup", "OwnerGroup"),
        Index("Status", "Status"),
        Index("MinorStatus", "MinorStatus"),
        Index("ApplicationStatus", "ApplicationStatus"),
        Index("StatusSite", "Status", "Site"),
        Index("LastUpdateTime", "LastUpdateTime"),
    )


class JobJDLs(JobDBBase):
    __tablename__ = "JobJDLs"
    JobID = Column(Integer, autoincrement=True, primary_key=True)
    JDL = Column(Text)
    JobRequirements = Column(Text)
    OriginalJDL = Column(Text)


class InputData(JobDBBase):
    __tablename__ = "InputData"
    JobID = Column(
        Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    LFN = Column(String(255), default="", primary_key=True)
    Status = Column(String(32), default="AprioriGood")


class JobParameters(JobDBBase):
    __tablename__ = "JobParameters"
    JobID = Column(
        Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    Name = Column(String(100), primary_key=True)
    Value = Column(Text)


class OptimizerParameters(JobDBBase):
    __tablename__ = "OptimizerParameters"
    JobID = Column(
        Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    Name = Column(String(100), primary_key=True)
    Value = Column(Text)


class AtticJobParameters(JobDBBase):
    __tablename__ = "AtticJobParameters"
    JobID = Column(
        Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    Name = Column(String(100), primary_key=True)
    Value = Column(Text)
    RescheduleCycle = Column(Integer)


class SiteMask(JobDBBase):
    __tablename__ = "SiteMask"
    Site = Column(String(64), primary_key=True)
    Status = Column(String(64))
    LastUpdateTime = Column(DateTime)
    Author = Column(String(255))
    Comment = Column(Text)


class SiteMaskLogging(JobDBBase):
    __tablename__ = "SiteMaskLogging"
    Site = Column(String(64), primary_key=True)
    UpdateTime = Column(DateTime, primary_key=True)
    Status = Column(String(64))
    Author = Column(String(255))
    Comment = Column(Text)


class HeartBeatLoggingInfo(JobDBBase):
    __tablename__ = "HeartBeatLoggingInfo"
    JobID = Column(
        Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    Name = Column(String(100), primary_key=True)
    Value = Column(Text)
    HeartBeatTime = Column(DateTime, primary_key=True)


class JobCommands(JobDBBase):
    __tablename__ = "JobCommands"
    JobID = Column(
        Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    Command = Column(String(100))
    Arguments = Column(String(100))
    Status = Column(String(64), default="Received")
    ReceptionTime = Column(DateTime, primary_key=True)
    ExecutionTime = NullColumn(DateTime)
