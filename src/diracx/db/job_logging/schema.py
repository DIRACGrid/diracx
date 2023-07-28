from sqlalchemy import (
    DateTime,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    DDL,
    event
)
from sqlalchemy.orm import declarative_base

from ..utils import Column

Base = declarative_base()


class LoggingInfo(Base):
    __tablename__ = "LoggingInfo"
    JobID = Column(Integer)
    SeqNum = Column(Integer, default=0)
    Status = Column(String(32), default='')
    MinorStatus = Column(String(128), default='')
    ApplicationStatus = Column(String(255), default='')
    StatusTime = Column(DateTime)
    StatusTimeOrder = Column(Numeric(precision=12, scale=3))
    StatusSource = Column(String(32), default="Unknown")
    __table_args__ = (PrimaryKeyConstraint("JobID", "SeqNum"),)


# define the trigger

trigger = DDL(
    "CREATE TRIGGER SeqNumGenerator BEFORE INSERT ON LoggingInfo "
    "FOR EACH ROW SET NEW.SeqNum= (SELECT IFNULL(MAX(SeqNum) + 1,1) FROM LoggingInfo WHERE JobID=NEW.JobID);"
)

event.listen(LoggingInfo, "after_create", trigger.execute_if(dialect=("mysql")))