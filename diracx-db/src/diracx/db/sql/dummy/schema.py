"""SQLAlchemy schemas for the example owners and cars database."""

# The utils class define some boilerplate types that should be used
# in place of the SQLAlchemy one. Have a look at them
from __future__ import annotations

from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils import datetime_now, str255


class Base(DeclarativeBase):
    """Declarative base with shared SQLAlchemy type mappings.

    Attributes:
        type_annotation_map: SQL types associated with shared Python aliases.
    """

    type_annotation_map = {
        str255: String(255),
    }


class Owners(Base):
    """Owner records associated with cars.

    Attributes:
        owner_id: Database-generated owner identifier.
        creation_time: Time when the owner record was created.
        name: Owner's name.
    """

    __tablename__ = "Owners"
    owner_id: Mapped[int] = mapped_column(
        "OwnerID", primary_key=True, autoincrement=True
    )
    creation_time: Mapped[datetime_now] = mapped_column("CreationTime")
    name: Mapped[str255] = mapped_column("Name")


class Cars(Base):
    """Car records associated with their owners.

    Attributes:
        license_plate: Unique license plate identifier for the car.
        model: Car model name.
        owner_id: Identifier of the car's owner.
    """

    __tablename__ = "Cars"
    license_plate: Mapped[UUID] = mapped_column("LicensePlate", primary_key=True)
    model: Mapped[str255] = mapped_column("Model")
    owner_id: Mapped[int] = mapped_column("OwnerID", ForeignKey(Owners.owner_id))
