"""Sandbox metadata SQLAlchemy schema definitions.

This module defines the sandbox metadata ORM tables used for sandbox owner
records, sandbox registration metadata, and sandbox-to-job entity mappings.
"""

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
    """Base declarative class for sandbox metadata tables.

    The :attr:`type_annotation_map` maps compact string aliases such as
    ``str32`` to concrete SQLAlchemy ``String`` column types.
    """

    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str512: String(512),
    }


class SBOwners(Base):
    """ORM mapping for sandbox owner records.

    Attributes:
        OwnerID (int): Auto-incrementing primary key.
        Owner (str): Owner username.
        OwnerGroup (str): Owner group name.
        VO (str): Virtual organization.
    """

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
    """ORM mapping for sandbox metadata records.

    Attributes:
        SBId (int): Auto-incrementing primary key.
        OwnerId (int): Owner identifier.
        SEName (str): Storage element name.
        SEPFN (str): Physical file name.
        Bytes (int): Sandbox size in bytes.
        RegistrationTime (datetime): Sandbox registration timestamp.
        LastAccessTime (datetime): Last access timestamp.
        Assigned (bool): Whether the sandbox is currently assigned.
    """

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
        UniqueConstraint("SEName", "SEPFN", name="Location"),
    )


class SBEntityMapping(Base):
    """ORM mapping for sandbox-to-entity mappings.

    Attributes:
        SBId (int): Sandbox identifier.
        EntityId (str): Entity identifier.
        Type (str): Sandbox type.
    """

    __tablename__ = "sb_EntityMapping"
    SBId: Mapped[int]
    EntityId: Mapped[str128]
    Type: Mapped[str64]
    __table_args__ = (
        PrimaryKeyConstraint("SBId", "EntityId", "Type"),
        Index("SBId", "EntityId"),
        UniqueConstraint("SBId", "EntityId", "Type", name="Mapping"),
    )
