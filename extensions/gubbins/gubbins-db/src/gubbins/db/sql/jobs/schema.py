from diracx.db.sql.job.db import JobDBBase
from diracx.db.sql.job.schema import AccountedFlagEnum, InputData
from diracx.db.sql.utils import Column, EnumBackedBool, NullColumn
from diracx.db.sql.utils.types import SmarterDateTime
from sqlalchemy import (
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)

# NOTE: We need to remove the original Jobs table from metadata because
# GubbinsJobs modifies an existing column (job_name size from 128 to 512).
# Simple inheritance works fine for ADDING columns (see GubbinsInputData below),
# but MODIFYING existing columns in SQLAlchemy requires replacing the table definition.
# If we don't remove the original, we get duplicate table/index conflicts.
if "Jobs" in JobDBBase.metadata.tables:
    JobDBBase.metadata.remove(JobDBBase.metadata.tables["Jobs"])


# Create a new external table for Gubbins-specific categories
class GubbinsCategories(JobDBBase):
    """External table for Gubbins-specific categories"""

    __tablename__ = "GubbinsCategories"

    category_id = Column("CategoryID", Integer, primary_key=True, autoincrement=True)
    category_name = Column("CategoryName", String(255), nullable=False)
    description = Column("Description", Text, default="")

    __table_args__ = (Index("CategoryName", "CategoryName"), {"extend_existing": True})


# GubbinsJobs: Replaces the Jobs table with modifications
# NOTE: We cannot use simple inheritance (class GubbinsJobs(Jobs)) because we're
# modifying an existing column (job_name). SQLAlchemy inheritance works for ADDING
# columns but not for MODIFYING them. So we must redefine the entire table.
class GubbinsJobs(JobDBBase):
    """Custom Jobs table with modified column specifications and foreign key to GubbinsCategories"""

    __tablename__ = "Jobs"
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
        {"extend_existing": True},
    )

    # All columns from the parent Jobs table (copied from diracx.db.sql.job.schema)
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

    # MODIFIED: job_name with VARCHAR(512) instead of VARCHAR(128)
    job_name = Column("JobName", String(512), default="Unknown")

    owner = Column("Owner", String(64), default="Unknown")
    owner_group = Column("OwnerGroup", String(128), default="Unknown")
    vo = Column("VO", String(32))
    submission_time = NullColumn(
        "SubmissionTime",
        SmarterDateTime(),
    )
    reschedule_time = NullColumn(
        "RescheduleTime",
        SmarterDateTime(),
    )
    last_update_time = NullColumn(
        "LastUpdateTime",
        SmarterDateTime(),
    )
    start_exec_time = NullColumn(
        "StartExecTime",
        SmarterDateTime(),
    )
    heart_beat_time = NullColumn(
        "HeartBeatTime",
        SmarterDateTime(),
    )
    end_exec_time = NullColumn(
        "EndExecTime",
        SmarterDateTime(),
    )
    status = Column("Status", String(32), default="Received")
    minor_status = Column("MinorStatus", String(128), default="Unknown")
    application_status = Column("ApplicationStatus", String(255), default="Unknown")
    user_priority = Column("UserPriority", Integer, default=0)
    reschedule_counter = Column("RescheduleCounter", Integer, default=0)
    verified_flag = Column("VerifiedFlag", EnumBackedBool(), default=False)
    accounted_flag = Column("AccountedFlag", AccountedFlagEnum(), default=False)

    # NEW: foreign key constraint to GubbinsCategories table
    category_id = NullColumn(
        "CategoryID", Integer, ForeignKey("GubbinsCategories.CategoryID")
    )


# GubbinsInputData: Extends InputData by ADDING a column
# NOTE: Simple inheritance works here because we're only ADDING a new column,
# not modifying an existing one. This is the preferred approach when possible.
class GubbinsInputData(InputData):
    """Extended InputData table with Adler checksum support"""

    __tablename__ = "InputData"
    __table_args__ = {"extend_existing": True}

    # New column for Adler checksum
    adler_checksum = Column("AdlerChecksum", String(75), nullable=True)


# You need to inherit from the declarative_base of the parent DB
class GubbinsInfo(JobDBBase):
    """An extra table with respect to Vanilla diracx JobDB"""

    __tablename__ = "GubbinsInfo"
    __table_args__ = {"extend_existing": True}

    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    info = Column("Info", String(255), default="", primary_key=True)
