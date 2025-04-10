from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

from diracx.db.sql.utils import Column, DateNowColumn

Base = declarative_base()


class SBOwners(Base):
    __tablename__ = "sb_Owners"
    OwnerID = Column(Integer, autoincrement=True)
    Owner = Column(String(32))
    OwnerGroup = Column(String(32))
    VO = Column(String(64))
    __table_args__ = (
        PrimaryKeyConstraint("OwnerID"),
        UniqueConstraint("Owner", "OwnerGroup", "VO", name="unique_owner_group_vo"),
    )


class SandBoxes(Base):
    __tablename__ = "sb_SandBoxes"
    SBId = Column(Integer, autoincrement=True)
    OwnerId = Column(Integer)
    SEName = Column(String(64))
    SEPFN = Column(String(512))
    Bytes = Column(BigInteger)
    RegistrationTime = DateNowColumn()
    LastAccessTime = DateNowColumn()
    Assigned = Column(Boolean, default=False)
    __table_args__ = (
        PrimaryKeyConstraint("SBId"),
        Index("OwnerId", OwnerId),
        UniqueConstraint("SEName", "SEPFN", name="Location"),
    )


class SBEntityMapping(Base):
    __tablename__ = "sb_EntityMapping"
    SBId = Column(Integer)
    EntityId = Column(String(128))
    Type = Column(String(64))
    __table_args__ = (
        PrimaryKeyConstraint("SBId", "EntityId", "Type"),
        Index("SBId", "EntityId"),
        UniqueConstraint("SBId", "EntityId", "Type", name="Mapping"),
    )
