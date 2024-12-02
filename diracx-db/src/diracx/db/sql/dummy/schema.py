# The utils class define some boilerplate types that should be used
# in place of the SQLAlchemy one. Have a look at them
from sqlalchemy import ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import declarative_base

from diracx.db.sql.utils import Column, DateNowColumn

Base = declarative_base()


class Owners(Base):
    __tablename__ = "Owners"
    owner_id = Column(Integer, primary_key=True, autoincrement=True)
    creation_time = DateNowColumn()
    name = Column(String(255))


class Cars(Base):
    __tablename__ = "Cars"
    license_plate = Column(Uuid(), primary_key=True)
    model = Column(String(255))
    owner_id = Column(Integer, ForeignKey(Owners.owner_id))
