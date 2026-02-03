# The utils class define some boilerplate types that should be used
# in place of the SQLAlchemy one. Have a look at them
from __future__ import annotations

from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils import datetime_now, str255


class Base(DeclarativeBase):
    type_annotation_map = {
        str255: String(255),
    }


class Owners(Base):
    __tablename__ = "Owners"
    owner_id: Mapped[int] = mapped_column(
        "OwnerID", primary_key=True, autoincrement=True
    )
    creation_time: Mapped[datetime_now] = mapped_column("CreationTime")
    name: Mapped[str255] = mapped_column("Name")


class Cars(Base):
    __tablename__ = "Cars"
    license_plate: Mapped[UUID] = mapped_column("LicensePlate", primary_key=True)
    model: Mapped[str255] = mapped_column("Model")
    owner_id: Mapped[int] = mapped_column("OwnerID", ForeignKey(Owners.owner_id))
