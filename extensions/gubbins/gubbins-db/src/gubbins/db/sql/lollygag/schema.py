"""Schema of the Lollygag example database."""

from __future__ import annotations

from uuid import UUID

# The utils module defines some boilerplate types that should be used
# in place of the SQLAlchemy ones. Have a look at them.
from diracx.db.sql.utils import datetime_now, str255
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base of the Lollygag tables."""

    type_annotation_map = {
        str255: String(255),
    }
    """Maps the ``str255`` type to a ``String(255)`` column."""


class Owners(Base):
    """Owners of the cars."""

    __tablename__ = "Owners"
    owner_id: Mapped[int] = mapped_column(
        "OwnerID", primary_key=True, autoincrement=True
    )
    """Unique identifier of the owner."""

    creation_time: Mapped[datetime_now] = mapped_column("CreationTime")
    """Time at which the owner was created."""

    name: Mapped[str255] = mapped_column("Name")
    """Name of the owner."""


class Cars(Base):
    """Cars, each belonging to an owner."""

    __tablename__ = "Cars"
    license_plate: Mapped[UUID] = mapped_column("LicensePlate", primary_key=True)
    """Unique identifier of the car."""

    model: Mapped[str255] = mapped_column("Model")
    """Model of the car."""

    owner_id: Mapped[int] = mapped_column("OwnerID", ForeignKey(Owners.owner_id))
    """Identifier of the owner of the car."""
