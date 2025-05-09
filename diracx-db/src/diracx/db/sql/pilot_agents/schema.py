from __future__ import annotations

from sqlalchemy import (
    DateTime,
    Double,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, DateNowColumn, EnumBackedBool, NullColumn

PilotAgentsDBBase = declarative_base()


class PilotAgents(PilotAgentsDBBase):
    __tablename__ = "PilotAgents"

    pilot_id = Column("PilotID", Integer, autoincrement=True, primary_key=True)
    initial_job_id = Column("InitialJobID", Integer, default=0)
    current_job_id = Column("CurrentJobID", Integer, default=0)
    pilot_job_reference = Column("PilotJobReference", String(255), default="Unknown")
    pilot_stamp = Column("PilotStamp", String(32), default="")
    destination_site = Column("DestinationSite", String(128), default="NotAssigned")
    queue = Column("Queue", String(128), default="Unknown")
    grid_site = Column("GridSite", String(128), default="Unknown")
    vo = Column("VO", String(128))
    grid_type = Column("GridType", String(32), default="LCG")
    benchmark = Column("BenchMark", Double, default=0.0)
    submission_time = NullColumn("SubmissionTime", DateTime)
    last_update_time = NullColumn("LastUpdateTime", DateTime)
    status = Column("Status", String(32), default="Unknown")
    status_reason = Column("StatusReason", String(255), default="Unknown")
    accounting_sent = Column("AccountingSent", EnumBackedBool(), default=False)

    __table_args__ = (
        Index("PilotJobReference", "PilotJobReference"),
        Index("Status", "Status"),
        Index("Statuskey", "GridSite", "DestinationSite", "Status"),
    )


class JobToPilotMapping(PilotAgentsDBBase):
    __tablename__ = "JobToPilotMapping"

    pilot_id = Column("PilotID", Integer, primary_key=True)
    job_id = Column("JobID", Integer, primary_key=True)
    start_time = Column("StartTime", DateTime)

    __table_args__ = (Index("JobID", "JobID"), Index("PilotID", "PilotID"))


class PilotOutput(PilotAgentsDBBase):
    __tablename__ = "PilotOutput"

    pilot_id = Column("PilotID", Integer, primary_key=True)
    std_output = Column("StdOutput", Text)
    std_error = Column("StdError", Text)


class PilotSecrets(PilotAgentsDBBase):
    __tablename__ = "PilotSecrets"

    secret_id = Column("SecretID", Integer, primary_key=True)
    hashed_secret = Column("HashedSecret", String(64))
    # Global count
    secret_global_use_count = Column("SecretGlobalUseCount", SmallInteger, default=0)
    # Null: Infinite use
    secret_global_use_count_max = NullColumn(
        "SecretGlobalUseCountMax", SmallInteger, default=1
    )
    secret_creation_time = DateNowColumn("SecretCreationDate")
    secret_expiration_date = NullColumn("SecretExpirationDate", DateTime(timezone=True))
    # To authorize only pilots from a specific VO to access a secret
    # Null VO => Can be used by everyone
    secret_vo = NullColumn("SecretVO", String(128))

    __table_args__ = (UniqueConstraint("HashedSecret", name="uq_hashed_secret"),)


class PilotToSecretMapping(PilotAgentsDBBase):
    """Map multiple pilots to multiple secrets. Allow secret reuse."""

    __tablename__ = "PilotToSecretMapping"

    # Primary key is (PilotSecretID, PilotStamp) pair
    pilot_secret_id = Column(
        "PilotSecretID",
        Integer,
        ForeignKey("PilotSecrets.SecretID", ondelete="CASCADE"),
        primary_key=True,
    )
    pilot_stamp = Column("PilotStamp", String(32), primary_key=True)
    # Different from global use: only counts how many a specific pilot used a specific secret
    pilot_secret_use_count = Column("PilotSecretUseCount", SmallInteger, default=0)
    pilot_secret_last_use_time = NullColumn(
        "PilotSecretLastUseDate", DateTime(timezone=True)
    )
