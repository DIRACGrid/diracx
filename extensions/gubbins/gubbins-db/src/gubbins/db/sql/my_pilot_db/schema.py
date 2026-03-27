from __future__ import annotations

from enum import StrEnum, auto

from diracx.db.sql.utils import datetime_now, str255
from sqlalchemy import Float, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class MyPilotStatus(StrEnum):
    SUBMITTED = auto()
    RUNNING = auto()
    DONE = auto()
    FAILED = auto()


class Base(DeclarativeBase):
    type_annotation_map = {
        str255: String(255),
    }


class MyComputeElements(Base):
    __tablename__ = "MyComputeElements"
    name: Mapped[str255] = mapped_column("Name", primary_key=True)
    capacity: Mapped[int] = mapped_column("Capacity")
    success_rate: Mapped[float] = mapped_column("SuccessRate", type_=Float)
    enabled: Mapped[bool] = mapped_column("Enabled")


class MyPilotSubmissions(Base):
    __tablename__ = "MyPilotSubmissions"
    pilot_id: Mapped[int] = mapped_column(
        "PilotID", primary_key=True, autoincrement=True
    )
    ce_name: Mapped[str255] = mapped_column(
        "CEName", ForeignKey(MyComputeElements.name)
    )
    status: Mapped[str255] = mapped_column("Status")
    submitted_at: Mapped[datetime_now] = mapped_column("SubmittedAt")
    updated_at: Mapped[datetime_now] = mapped_column("UpdatedAt")
