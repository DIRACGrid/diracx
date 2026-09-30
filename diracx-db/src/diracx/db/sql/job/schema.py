"""SQLAlchemy table schemas for job data and related records."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import sqlalchemy.types as types
from sqlalchemy import (
    ForeignKey,
    Index,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from typing_extensions import Annotated

from diracx.db.sql.utils.types import SmarterDateTime

from ..utils import EnumBackedBool, str32, str64, str128, str255

str100 = Annotated[str, 100]


class JobDBBase(DeclarativeBase):
    """Declarative base with shared string-column type mappings.

    Attributes:
        type_annotation_map: SQL types associated with the shared string aliases.
    """

    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str100: String(100),
        str128: String(128),
        str255: String(255),
    }


class AccountedFlagEnum(types.TypeDecorator):
    """Map the accounted-flag database enum to Python values.

    Attributes:
        impl: SQLAlchemy enum representation stored in the database.
        cache_ok: Whether SQLAlchemy may cache statements using this type.
    """

    impl = types.Enum("True", "False", "Failed", name="accounted_flag_enum")
    cache_ok = True

    def process_bind_param(self, value, dialect) -> str:
        """Convert a Python accounted flag to its database representation.

        Args:
            value: Boolean or ``"Failed"`` value to store.
            dialect: SQLAlchemy dialect handling the bind operation.

        Returns:
            The corresponding string stored in the database.

        Raises:
            NotImplementedError: If the value is not supported.
        """
        if value is True:
            return "True"
        elif value is False:
            return "False"
        elif value == "Failed":
            return "Failed"
        else:
            raise NotImplementedError(value, dialect)

    def process_result_value(self, value, dialect) -> bool | str:
        """Convert a database accounted flag to its Python representation.

        Args:
            value: String value read from the database.
            dialect: SQLAlchemy dialect handling the result.

        Returns:
            A boolean for ``"True"`` or ``"False"``, otherwise ``"Failed"``.

        Raises:
            NotImplementedError: If the database value is unknown.
        """
        if value == "True":
            return True
        elif value == "False":
            return False
        elif value == "Failed":
            return "Failed"
        else:
            raise NotImplementedError(f"Unknown {value=}")


class Jobs(JobDBBase):
    """Primary job records and their current status and attributes.

    Attributes:
        job_id: Identifier linking the job to its JDL record.
        job_type: Type of the job.
        job_group: Group associated with the job.
        site: Site assigned to the job.
        job_name: Name of the job.
        owner: User who owns the job.
        owner_group: Group of the job owner.
        vo: Virtual organization associated with the job.
        submission_time: Time when the job was submitted.
        reschedule_time: Time of the most recent rescheduling.
        last_update_time: Time of the most recent job update.
        start_exec_time: Time when execution started.
        heart_beat_time: Time of the latest heartbeat.
        end_exec_time: Time when execution ended.
        status: Current job status.
        minor_status: More detailed job status.
        application_status: Application-specific status.
        user_priority: Priority assigned to the job.
        reschedule_counter: Number of times the job was rescheduled.
        verified_flag: Whether the job has been verified.
        accounted_flag: Accounting state of the job.
    """

    __tablename__ = "Jobs"

    job_id: Mapped[int] = mapped_column(
        "JobID",
        ForeignKey("JobJDLs.JobID", ondelete="CASCADE"),
        primary_key=True,
        default=0,
    )
    job_type: Mapped[str32] = mapped_column("JobType", default="user")
    job_group: Mapped[str32] = mapped_column("JobGroup", default="00000000")
    site: Mapped[str100] = mapped_column("Site", default="ANY")
    job_name: Mapped[str128] = mapped_column("JobName", default="Unknown")
    owner: Mapped[str64] = mapped_column("Owner", default="Unknown")
    owner_group: Mapped[str128] = mapped_column("OwnerGroup", default="Unknown")
    vo: Mapped[str32] = mapped_column("VO")
    submission_time: Mapped[Optional[datetime]] = mapped_column(
        "SubmissionTime",
        SmarterDateTime(),
    )
    reschedule_time: Mapped[Optional[datetime]] = mapped_column(
        "RescheduleTime",
        SmarterDateTime(),
    )
    last_update_time: Mapped[Optional[datetime]] = mapped_column(
        "LastUpdateTime",
        SmarterDateTime(),
    )
    start_exec_time: Mapped[Optional[datetime]] = mapped_column(
        "StartExecTime",
        SmarterDateTime(),
    )
    heart_beat_time: Mapped[Optional[datetime]] = mapped_column(
        "HeartBeatTime",
        SmarterDateTime(),
    )
    end_exec_time: Mapped[Optional[datetime]] = mapped_column(
        "EndExecTime",
        SmarterDateTime(),
    )
    status: Mapped[str32] = mapped_column("Status", default="Received")
    minor_status: Mapped[str128] = mapped_column("MinorStatus", default="Unknown")
    application_status: Mapped[str255] = mapped_column(
        "ApplicationStatus", default="Unknown"
    )
    user_priority: Mapped[int] = mapped_column("UserPriority", default=0)
    reschedule_counter: Mapped[int] = mapped_column("RescheduleCounter", default=0)
    verified_flag: Mapped[bool] = mapped_column(
        "VerifiedFlag", EnumBackedBool(), default=False
    )
    accounted_flag: Mapped[bool | str] = mapped_column(
        "AccountedFlag", AccountedFlagEnum(), default=False
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
    """JDL documents and requirements associated with jobs.

    Attributes:
        job_id: Identifier assigned to the job.
        jdl: Current job description language document.
        job_requirements: Requirements extracted from the JDL.
        original_jdl: Original submitted JDL document.
    """

    __tablename__ = "JobJDLs"
    job_id: Mapped[int] = mapped_column("JobID", autoincrement=True, primary_key=True)
    jdl: Mapped[str] = mapped_column("JDL", Text)
    job_requirements: Mapped[str] = mapped_column("JobRequirements", Text)
    original_jdl: Mapped[str] = mapped_column("OriginalJDL", Text)


class InputData(JobDBBase):
    """Input logical file names associated with jobs.

    Attributes:
        job_id: Identifier of the job requiring the input file.
        lfn: Logical file name of the input file.
        status: Status of the input data entry.
    """

    __tablename__ = "InputData"
    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    lfn: Mapped[str255] = mapped_column("LFN", default="", primary_key=True)
    status: Mapped[str32] = mapped_column("Status", default="AprioriGood")


class JobParameters(JobDBBase):
    """Additional parameter values associated with jobs.

    Attributes:
        job_id: Identifier of the job.
        name: Parameter name.
        value: Parameter value stored as text.
    """

    __tablename__ = "JobParameters"
    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name: Mapped[str100] = mapped_column("Name", primary_key=True)
    value: Mapped[str] = mapped_column("Value", Text)


class OptimizerParameters(JobDBBase):
    """Optimizer parameter values associated with jobs.

    Attributes:
        job_id: Identifier of the job.
        name: Optimizer parameter name.
        value: Parameter value stored as text.
    """

    __tablename__ = "OptimizerParameters"
    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name: Mapped[str100] = mapped_column("Name", primary_key=True)
    value: Mapped[str] = mapped_column("Value", Text)


class AtticJobParameters(JobDBBase):
    """Archived job parameter values retained across rescheduling.

    Attributes:
        job_id: Identifier of the job.
        name: Archived parameter name.
        value: Parameter value stored as text.
        reschedule_cycle: Rescheduling cycle associated with the value.
    """

    __tablename__ = "AtticJobParameters"
    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name: Mapped[str100] = mapped_column("Name", primary_key=True)
    value: Mapped[str] = mapped_column("Value", Text)
    reschedule_cycle: Mapped[int] = mapped_column("RescheduleCycle")


class HeartBeatLoggingInfo(JobDBBase):
    """Dynamic resource information reported by job heartbeats.

    Attributes:
        job_id: Identifier of the reporting job.
        name: Name of the heartbeat field.
        value: Heartbeat field value stored as text.
        heart_beat_time: Time when the heartbeat was received.
    """

    __tablename__ = "HeartBeatLoggingInfo"
    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    name: Mapped[str100] = mapped_column("Name", primary_key=True)
    value: Mapped[str] = mapped_column("Value", Text)
    heart_beat_time: Mapped[datetime] = mapped_column(
        "HeartBeatTime",
        SmarterDateTime(),
        primary_key=True,
    )


class JobCommands(JobDBBase):
    """Commands queued for delivery to jobs during a heartbeat.

    Attributes:
        job_id: Identifier of the target job.
        command: Command to deliver.
        arguments: Arguments associated with the command.
        status: Delivery state of the command.
        reception_time: Time when the command was queued.
        execution_time: Time when the command was executed, if known.
    """

    __tablename__ = "JobCommands"
    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    command: Mapped[str100] = mapped_column("Command")
    arguments: Mapped[str100] = mapped_column("Arguments")
    status: Mapped[str64] = mapped_column("Status", default="Received")
    reception_time: Mapped[datetime] = mapped_column(
        "ReceptionTime",
        SmarterDateTime(),
        primary_key=True,
    )
    execution_time: Mapped[Optional[datetime]] = mapped_column(
        "ExecutionTime",
        SmarterDateTime(),
    )
