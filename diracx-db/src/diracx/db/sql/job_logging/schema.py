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
    JobID = Column(Integer)
    SeqNum = Column(Integer)
    Status = Column(String(32), default="")
    MinorStatus = Column(String(128), default="")
    ApplicationStatus = Column(String(255), default="")
    StatusTime = DateNowColumn()
    # TODO: Check that this corresponds to the DOUBLE(12,3) type in MySQL
    StatusTimeOrder = Column(Numeric(precision=12, scale=3), default=0)
    Source = Column(String(32), default="Unknown", name="StatusSource")
    __table_args__ = (PrimaryKeyConstraint("JobID", "SeqNum"),)
