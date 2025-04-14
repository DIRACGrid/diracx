from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Integer, Numeric, PrimaryKeyConstraint, String, TypeDecorator
from sqlalchemy.orm import declarative_base

from ..utils import Column, DateNowColumn

JobLoggingDBBase = declarative_base()


class MagicEpochDateTime(TypeDecorator):
    """A SQLAlchemy type that stores a datetime as a numeric value representing the
    seconds elapsed since MAGIC_EPOC_NUMBER. The underlying column is defined as
    Numeric(12,3) which provides a fixed-precision representation.
    """

    impl = Numeric(12, 3)
    cache_ok = True

    MAGIC_EPOC_NUMBER = 1270000000

    def process_bind_param(self, value, dialect):
        """Convert a Python datetime to a numeric value: (timestamp - MAGIC_EPOC_NUMBER).
        The result is rounded to three decimal places.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            # Convert datetime to seconds since the Unix epoch, subtract our magic epoch,
            # and round to three decimal places.
            epoch_seconds = (
                value.replace(tzinfo=UTC).timestamp() - self.MAGIC_EPOC_NUMBER
            )
            return round(epoch_seconds, 3)
        raise ValueError(
            "Expected a datetime object for MagicEpochDateTime bind parameter."
        )

    def process_result_value(self, value, dialect):
        """Convert the numeric database value back into a Python datetime by reversing the
        stored difference (adding MAGIC_EPOC_NUMBER).
        """
        if value is None:
            return None
        # Carefully convert from Decimal to datetime to avoid loosing precision
        value += self.MAGIC_EPOC_NUMBER
        value_int = int(value)
        result = datetime.fromtimestamp(value_int, tz=UTC)
        return result.replace(microsecond=int((value - value_int) * 1_000_000))


class LoggingInfo(JobLoggingDBBase):
    __tablename__ = "LoggingInfo"
    job_id = Column("JobID", Integer)
    seq_num = Column("SeqNum", Integer)
    status = Column("Status", String(32), default="")
    minor_status = Column("MinorStatus", String(128), default="")
    application_status = Column("ApplicationStatus", String(255), default="")
    status_time = DateNowColumn("StatusTime")
    status_time_order = Column("StatusTimeOrder", MagicEpochDateTime, default=0)
    source = Column("StatusSource", String(32), default="Unknown")
    __table_args__ = (PrimaryKeyConstraint("JobID", "SeqNum"),)
