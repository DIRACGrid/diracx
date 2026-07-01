"""SQLAlchemy schema and helper types for job logging.

This module defines the ORM mappings and custom SQLAlchemy types used by
the job logging subsystem: the declarative base, a numeric-epoch-backed
datetime type and the ``LoggingInfo`` table mapping.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Numeric, PrimaryKeyConstraint, String, TypeDecorator
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..utils import datetime_now, str32, str128, str255


class JobLoggingDBBase(DeclarativeBase):
    """Base declarative class for the job logging schema.

    This base class provides a :attr:`type_annotation_map` used by mapped
    classes in this module to allow compact annotation aliases such as
    ``str32`` while producing concrete SQLAlchemy column types.

    Attributes:
        type_annotation_map (dict): Mapping from annotation aliases to
            SQLAlchemy column types.
    """

    type_annotation_map = {
        str32: String(32),
        str128: String(128),
        str255: String(255),
    }


class MagicEpochDateTime(TypeDecorator):
    """SQLAlchemy type that stores datetimes as a numeric value.

    The stored value represents the seconds elapsed since ``MAGIC_EPOC_NUMBER``.
    The underlying column is defined as ``Numeric(12,3)`` which provides a
    fixed-precision representation.

    Attributes:
        MAGIC_EPOC_NUMBER (int): Base epoch used for stored values.
    """

    impl = Numeric(12, 3)
    cache_ok = True

    MAGIC_EPOC_NUMBER = 1270000000

    def process_bind_param(self, value, dialect):
        """Convert a Python datetime to a numeric DB value.

        Args:
            value (datetime | None): The datetime to convert. If ``None``, ``None``
                is returned.
            dialect: SQLAlchemy dialect (unused).

        Returns:
            float | None: Seconds since ``MAGIC_EPOC_NUMBER``, rounded to three
                decimal places, or ``None`` when ``value`` is ``None``.
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
        """Convert the numeric DB value back into a Python ``datetime``.

        Args:
            value (Decimal | None): Numeric value read from the DB (seconds
                since ``MAGIC_EPOC_NUMBER``).
            dialect: SQLAlchemy dialect (unused).

        Returns:
            datetime | None: A timezone-aware ``datetime`` in UTC, or ``None`` if
            ``value`` is ``None``.
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
    """ORM mapping for job logging entries.

    Each row records a state change observed for a job. The primary key is
    the composite (JobID, SeqNum).

    Attributes:
        job_id (int): Job identifier.
        seq_num (int): Sequence number for ordered events per job.
        status (str): Major state name.
        minor_status (str): Minor state description.
        application_status (str): Application-provided status (up to 255 chars).
        status_time (datetime): Time when the status event occurred.
        status_time_order (datetime): Ordering timestamp stored via
            ``MagicEpochDateTime`` for stable ordering.
        source (str): Event source identifier.
    """
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
