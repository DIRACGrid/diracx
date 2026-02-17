from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Index,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils import datetime_now, str32, str64, str128, str512


class Base(DeclarativeBase):
    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str512: String(512),
    }


class SBOwners(Base):
    __tablename__ = "sb_Owners"
    OwnerID: Mapped[int] = mapped_column(autoincrement=True)
    Owner: Mapped[str32]
    OwnerGroup: Mapped[str32]
    VO: Mapped[str64]
    __table_args__ = (
        PrimaryKeyConstraint("OwnerID"),
        UniqueConstraint("Owner", "OwnerGroup", "VO", name="unique_owner_group_vo"),
    )


class SandBoxes(Base):
    __tablename__ = "sb_SandBoxes"
    SBId: Mapped[int] = mapped_column(autoincrement=True)
    OwnerId: Mapped[int]
    SEName: Mapped[str64]
    SEPFN: Mapped[str512]
    Bytes: Mapped[int] = mapped_column(BigInteger)
    RegistrationTime: Mapped[datetime_now]
    LastAccessTime: Mapped[datetime_now]
    Assigned: Mapped[bool] = mapped_column(default=False)
    __table_args__ = (
        PrimaryKeyConstraint("SBId"),
        Index("OwnerId", "OwnerId"),
        Index("idx_assigned_lastaccesstime", "Assigned", "LastAccessTime"),
        UniqueConstraint("SEName", "SEPFN", name="Location"),
    )


class SBEntityMapping(Base):
    __tablename__ = "sb_EntityMapping"
    SBId: Mapped[int]
    EntityId: Mapped[str128]
    Type: Mapped[str64]
    __table_args__ = (
        PrimaryKeyConstraint("SBId", "EntityId", "Type"),
        Index("SBId", "EntityId"),
        UniqueConstraint("SBId", "EntityId", "Type", name="Mapping"),
    )
