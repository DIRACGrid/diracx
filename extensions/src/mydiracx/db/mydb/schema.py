from enum import Enum, auto

from sqlalchemy import (
    DateTime,
    Integer,
    JSON,
    String,
    Uuid,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import declarative_base

from diracx.db.utils import Column, DateNowColumn, EnumColumn, NullColumn

USER_CODE_LENGTH = 8

Base = declarative_base()

class CustomObject(Base):
    __tablename__ = "CustomObject"
    ID = Column(Integer, autoincrement=True, primary_key=True)
    PathValueAsString = Column(String(255))
    IntegerValue = Column(Integer)
    InitialUpdate = Column(DateTime, default=None)
    LastUpdate = Column(DateTime, default=None)
    __table_args__ = (PrimaryKeyConstraint("ID"),)
