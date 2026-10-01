"""SQLAlchemy schemas for sandbox ownership, metadata, and job mappings."""

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
    """Declarative base mapping string aliases to SQL column types.

    Attributes:
        type_annotation_map: SQL types associated with the string aliases.
    """

    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str512: String(512),
    }


class SBOwners(Base):
    """Sandbox owner records.

    Attributes:
        OwnerID: Database identifier for the owner.
        Owner: Owner's username.
        OwnerGroup: Owner's group name.
        VO: Virtual organization associated with the owner.
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
    """Sandbox metadata records stored on storage elements.

    Attributes:
        SBId: Database identifier for the sandbox.
        OwnerId: Identifier of the sandbox owner.
        SEName: Storage element containing the sandbox.
        SEPFN: Physical file name of the sandbox.
        Bytes: Sandbox size in bytes.
        RegistrationTime: Time when the sandbox was registered.
        LastAccessTime: Time of the sandbox's most recent access.
        Assigned: Whether the sandbox is assigned to an entity.
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
    """Associations between sandboxes and entities such as jobs.

    Attributes:
        SBId: Identifier of the sandbox.
        EntityId: Identifier of the associated entity.
        Type: Type of the associated entity.
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
