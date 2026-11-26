"""Schema of the example database managing pilot submissions."""

from __future__ import annotations

from enum import StrEnum, auto

from diracx.db.sql.utils import datetime_now, str255
from sqlalchemy import Float, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class MyPilotStatus(StrEnum):
    """Status of a pilot submission."""

    SUBMITTED = auto()
    """The pilot was submitted to a compute element."""

    RUNNING = auto()
    """The pilot is running."""

    DONE = auto()
    """The pilot finished successfully."""

    FAILED = auto()
    """The pilot failed."""


class Base(DeclarativeBase):
    """Declarative base of the pilot submission tables."""

    type_annotation_map = {
        str255: String(255),
    }
    """Maps the ``str255`` type to a ``String(255)`` column."""


class MyComputeElements(Base):
    """Compute elements to which pilots can be submitted."""

    __tablename__ = "MyComputeElements"
    name: Mapped[str255] = mapped_column("Name", primary_key=True)
    """Name of the compute element."""

    capacity: Mapped[int] = mapped_column("Capacity")
    """Maximum number of active pilots."""

    success_rate: Mapped[float] = mapped_column("SuccessRate", type_=Float)
    """Reliability of the compute element, as the probability (0.0 to 1.0) that a pilot succeeds."""

    enabled: Mapped[bool] = mapped_column("Enabled")
    """Whether pilots can be submitted to the compute element."""


class MyPilotSubmissions(Base):
    """Pilots submitted to the compute elements."""

    __tablename__ = "MyPilotSubmissions"
    pilot_id: Mapped[int] = mapped_column(
        "PilotID", primary_key=True, autoincrement=True
    )
    """Unique identifier of the pilot submission."""

    ce_name: Mapped[str255] = mapped_column(
        "CEName", ForeignKey(MyComputeElements.name)
    )
    """Name of the compute element the pilot was submitted to."""

    status: Mapped[str255] = mapped_column("Status")
    """Status of the pilot, see ``MyPilotStatus``."""

    submitted_at: Mapped[datetime_now] = mapped_column("SubmittedAt")
    """Time at which the pilot was submitted."""

    updated_at: Mapped[datetime_now] = mapped_column("UpdatedAt")
    """Time at which the pilot was last updated."""
