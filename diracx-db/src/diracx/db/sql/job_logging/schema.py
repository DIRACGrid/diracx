from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Numeric, PrimaryKeyConstraint, String, TypeDecorator
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..utils import datetime_now, str32, str128, str255


class JobLoggingDBBase(DeclarativeBase):
    type_annotation_map = {
        str32: String(32),
        str128: String(128),
        str255: String(255),
    }


class MagicEpochDateTime(TypeDecorator):
    """A SQLAlchemy type to store a datetime as a numeric value.

    Representing the seconds elapsed since MAGIC_EPOC_NUMBER. The underlying column is defined as
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
        """Convert the numeric database value back into a Python datetime.

        Reversing the stored difference (adding MAGIC_EPOC_NUMBER).
        """
        if value is None:
            return None
        # Carefully convert from Decimal to datetime to avoid losing precision
        value += self.MAGIC_EPOC_NUMBER
        value_int = int(value)
        result = datetime.fromtimestamp(value_int, tz=UTC)
        return result.replace(microsecond=int((value - value_int) * 1_000_000))


class LoggingInfo(JobLoggingDBBase):
    __tablename__ = "LoggingInfo"
    job_id: Mapped[int] = mapped_column("JobID")
    seq_num: Mapped[int] = mapped_column("SeqNum")
    status: Mapped[str32] = mapped_column("Status", default="")
    minor_status: Mapped[str128] = mapped_column("MinorStatus", default="")
    application_status: Mapped[str255] = mapped_column("ApplicationStatus", default="")
    status_time: Mapped[datetime_now] = mapped_column("StatusTime")
    status_time_order: Mapped[datetime] = mapped_column(
        "StatusTimeOrder", MagicEpochDateTime(), default=0
    )
    source: Mapped[str32] = mapped_column("StatusSource", default="Unknown")
    __table_args__ = (PrimaryKeyConstraint("JobID", "SeqNum"),)
