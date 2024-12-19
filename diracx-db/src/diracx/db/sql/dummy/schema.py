# The utils class define some boilerplate types that should be used
# in place of the SQLAlchemy one. Have a look at them
from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import declarative_base

from diracx.db.sql.utils import Column, DateNowColumn

Base = declarative_base()


class Owners(Base):
    __tablename__ = "Owners"
    owner_id = Column("OwnerID", Integer, primary_key=True, autoincrement=True)
    creation_time = DateNowColumn("CreationTime")
    name = Column("Name", String(255))


class Cars(Base):
    __tablename__ = "Cars"
    license_plate = Column("LicensePlate", Uuid(), primary_key=True)
    model = Column("Model", String(255))
    owner_id = Column("OwnerID", Integer, ForeignKey(Owners.owner_id))
