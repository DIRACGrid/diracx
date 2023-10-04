import sqlalchemy.types as types
from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, DateNowColumn, NullColumn

JobDBBase = declarative_base()
JobLoggingDBBase = declarative_base()


class EnumBackedBool(types.TypeDecorator):
    """Maps a ``EnumBackedBool()`` column to True/False in Python"""

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


class JobJDLs(JobDBBase):
    __tablename__ = "JobJDLs"
    JobID = Column(Integer, autoincrement=True)
    JDL = Column(Text)
    JobRequirements = Column(Text)
    OriginalJDL = Column(Text)
    __table_args__ = (PrimaryKeyConstraint("JobID"),)


class Jobs(JobDBBase):
    __tablename__ = "Jobs"

    JobID = Column("JobID", Integer, primary_key=True, default=0)
    JobType = Column("JobType", String(32), default="user")
    JobGroup = Column("JobGroup", String(32), default="00000000")
    JobSplitType = Column(
        "JobSplitType", Enum("Single", "Master", "Subjob", "DAGNode"), default="Single"
    )
    MasterJobID = Column("MasterJobID", Integer, default=0)
    Site = Column("Site", String(100), default="ANY")
    JobName = Column("JobName", String(128), default="Unknown")
    Owner = Column("Owner", String(64), default="Unknown")
    OwnerGroup = Column("OwnerGroup", String(128), default="Unknown")
    SubmissionTime = NullColumn("SubmissionTime", DateTime)
    RescheduleTime = NullColumn("RescheduleTime", DateTime)
    LastUpdateTime = NullColumn("LastUpdateTime", DateTime)
    StartExecTime = NullColumn("StartExecTime", DateTime)
    HeartBeatTime = NullColumn("HeartBeatTime", DateTime)
    EndExecTime = NullColumn("EndExecTime", DateTime)
    Status = Column("Status", String(32), default="Received")
    MinorStatus = Column("MinorStatus", String(128), default="Unknown")
    ApplicationStatus = Column("ApplicationStatus", String(255), default="Unknown")
    ApplicationNumStatus = Column("ApplicationNumStatus", Integer, default=0)
    UserPriority = Column("UserPriority", Integer, default=0)
    SystemPriority = Column("SystemPriority", Integer, default=0)
    RescheduleCounter = Column("RescheduleCounter", Integer, default=0)
    VerifiedFlag = Column("VerifiedFlag", EnumBackedBool(), default=False)
    DeletedFlag = Column("DeletedFlag", EnumBackedBool(), default=False)
    KilledFlag = Column("KilledFlag", EnumBackedBool(), default=False)
    FailedFlag = Column("FailedFlag", EnumBackedBool(), default=False)
    ISandboxReadyFlag = Column("ISandboxReadyFlag", EnumBackedBool(), default=False)
    OSandboxReadyFlag = Column("OSandboxReadyFlag", EnumBackedBool(), default=False)
    RetrievedFlag = Column("RetrievedFlag", EnumBackedBool(), default=False)
    # TODO: Should this be True/False/"Failed"? Or True/False/Null?
    AccountedFlag = Column(
        "AccountedFlag", Enum("True", "False", "Failed"), default="False"
    )

    __table_args__ = (
        ForeignKeyConstraint(["JobID"], ["JobJDLs.JobID"]),
        Index("JobType", "JobType"),
        Index("JobGroup", "JobGroup"),
        Index("JobSplitType", "JobSplitType"),
        Index("Site", "Site"),
        Index("Owner", "Owner"),
        Index("OwnerGroup", "OwnerGroup"),
        Index("Status", "Status"),
        Index("MinorStatus", "MinorStatus"),
        Index("ApplicationStatus", "ApplicationStatus"),
        Index("StatusSite", "Status", "Site"),
        Index("LastUpdateTime", "LastUpdateTime"),
    )


class InputData(JobDBBase):
    __tablename__ = "InputData"
    JobID = Column(Integer, primary_key=True)
    LFN = Column(String(255), default="", primary_key=True)
    Status = Column(String(32), default="AprioriGood")
    __table_args__ = (ForeignKeyConstraint(["JobID"], ["Jobs.JobID"]),)


class JobParameters(JobDBBase):
    __tablename__ = "JobParameters"
    JobID = Column(Integer, primary_key=True)
    Name = Column(String(100), primary_key=True)
    Value = Column(Text)
    __table_args__ = (ForeignKeyConstraint(["JobID"], ["Jobs.JobID"]),)


class OptimizerParameters(JobDBBase):
    __tablename__ = "OptimizerParameters"
    JobID = Column(Integer, primary_key=True)
    Name = Column(String(100), primary_key=True)
    Value = Column(Text)
    __table_args__ = (ForeignKeyConstraint(["JobID"], ["Jobs.JobID"]),)


class AtticJobParameters(JobDBBase):
    __tablename__ = "AtticJobParameters"
    JobID = Column(Integer, ForeignKey("Jobs.JobID"), primary_key=True)
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
    JobID = Column(Integer, primary_key=True)
    Name = Column(String(100), primary_key=True)
    Value = Column(Text)
    HeartBeatTime = Column(DateTime, primary_key=True)

    __table_args__ = (ForeignKeyConstraint(["JobID"], ["Jobs.JobID"]),)


class JobCommands(JobDBBase):
    __tablename__ = "JobCommands"
    JobID = Column(Integer, primary_key=True)
    Command = Column(String(100))
    Arguments = Column(String(100))
    Status = Column(String(64), default="Received")
    ReceptionTime = Column(DateTime, primary_key=True)
    ExecutionTime = NullColumn(DateTime)

    __table_args__ = (ForeignKeyConstraint(["JobID"], ["Jobs.JobID"]),)


class LoggingInfo(JobLoggingDBBase):
    __tablename__ = "LoggingInfo"
    JobID = Column(Integer)
    SeqNum = Column(Integer)
    Status = Column(String(32), default="")
    MinorStatus = Column(String(128), default="")
    ApplicationStatus = Column(String(255), default="")
    StatusTime = DateNowColumn()
    # TODO: Check that this corresponds to the DOUBLE(12,3) type in MySQL
    StatusTimeOrder = Column(Numeric(precision=12, scale=3), default=0)
    StatusSource = Column(String(32), default="Unknown")
    __table_args__ = (PrimaryKeyConstraint("JobID", "SeqNum"),)
