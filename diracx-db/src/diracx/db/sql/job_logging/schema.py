from __future__ import annotations

from sqlalchemy import (
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, DateNowColumn

JobLoggingDBBase = declarative_base()


class LoggingInfo(JobLoggingDBBase):
    __tablename__ = "LoggingInfo"
    job_id = Column("JobID", Integer)
    seq_num = Column("SeqNum", Integer)
    status = Column("Status", String(32), default="")
    minor_status = Column("MinorStatus", String(128), default="")
    application_status = Column("ApplicationStatus", String(255), default="")
    status_time = DateNowColumn("StatusTime")
    # TODO: Check that this corresponds to the DOUBLE(12,3) type in MySQL
    status_time_order = Column(
        "StatusTimeOrder", Numeric(precision=12, scale=3), default=0
    )
    source = Column("StatusSource", String(32), default="Unknown")
    __table_args__ = (PrimaryKeyConstraint("JobID", "SeqNum"),)
