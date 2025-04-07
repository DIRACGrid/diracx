from __future__ import annotations

import sqlalchemy.types as types
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, EnumBackedBool, NullColumn

JobDBBase = declarative_base()


class AccountedFlagEnum(types.TypeDecorator):
    """Maps a ``AccountedFlagEnum()`` column to True/False in Python."""

    impl = types.Enum
    cache_ok: bool = True

    def __init__(self) -> None:
        super().__init__("True", "False", "Failed")

    def process_bind_param(self, value, dialect) -> str:
        if value is True:
            return "True"
        elif value is False:
            return "False"
        elif value == "Failed":
            return "Failed"
        else:
            raise NotImplementedError(value, dialect)

    def process_result_value(self, value, dialect) -> bool | str:
        if value == "True":
            return True
        elif value == "False":
            return False
        elif value == "Failed":
            return "Failed"
        else:
            raise NotImplementedError(f"Unknown {value=}")


class Jobs(JobDBBase):
    __tablename__ = "Jobs"

    job_id = Column(
        "JobID",
        Integer,
        ForeignKey("JobJDLs.JobID", ondelete="CASCADE"),
        primary_key=True,
        default=0,
    )
    job_type = Column("JobType", String(32), default="user")
    job_group = Column("JobGroup", String(32), default="00000000")
    site = Column("Site", String(100), default="ANY")
    job_name = Column("JobName", String(128), default="Unknown")
    owner = Column("Owner", String(64), default="Unknown")
    owner_group = Column("OwnerGroup", String(128), default="Unknown")
    vo = Column("VO", String(32))
    submission_time = NullColumn("SubmissionTime", DateTime)
    reschedule_time = NullColumn("RescheduleTime", DateTime)
    last_update_time = NullColumn("LastUpdateTime", DateTime)
    start_exec_time = NullColumn("StartExecTime", DateTime)
    heart_beat_time = NullColumn("HeartBeatTime", DateTime)
    end_exec_time = NullColumn("EndExecTime", DateTime)
    status = Column("Status", String(32), default="Received")
    minor_status = Column("MinorStatus", String(128), default="Unknown")
    application_status = Column("ApplicationStatus", String(255), default="Unknown")
    user_priority = Column("UserPriority", Integer, default=0)
    reschedule_counter = Column("RescheduleCounter", Integer, default=0)
    verified_flag = Column("VerifiedFlag", EnumBackedBool(), default=False)
    accounted_flag = Column("AccountedFlag", AccountedFlagEnum(), default=False)

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
    job_id = Column("JobID", Integer, autoincrement=True, primary_key=True)
    jdl = Column("JDL", Text)
    job_requirements = Column("JobRequirements", Text)
    original_jdl = Column("OriginalJDL", Text)


class InputData(JobDBBase):
    __tablename__ = "InputData"
    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    lfn = Column("LFN", String(255), default="", primary_key=True)
    status = Column("Status", String(32), default="AprioriGood")


class JobParameters(JobDBBase):
    __tablename__ = "JobParameters"
    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name = Column("Name", String(100), primary_key=True)
    value = Column("Value", Text)


class OptimizerParameters(JobDBBase):
    __tablename__ = "OptimizerParameters"
    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name = Column("Name", String(100), primary_key=True)
    value = Column("Value", Text)


class AtticJobParameters(JobDBBase):
    __tablename__ = "AtticJobParameters"
    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name = Column("Name", String(100), primary_key=True)
    value = Column("Value", Text)
    reschedule_cycle = Column("RescheduleCycle", Integer)


class HeartBeatLoggingInfo(JobDBBase):
    __tablename__ = "HeartBeatLoggingInfo"
    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name = Column("Name", String(100), primary_key=True)
    value = Column("Value", Text)
    heart_beat_time = Column("HeartBeatTime", DateTime, primary_key=True)


class JobCommands(JobDBBase):
    __tablename__ = "JobCommands"
    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    command = Column("Command", String(100))
    arguments = Column("Arguments", String(100))
    status = Column("Status", String(64), default="Received")
    reception_time = Column("ReceptionTime", DateTime, primary_key=True)
    execution_time = NullColumn("ExecutionTime", DateTime)
