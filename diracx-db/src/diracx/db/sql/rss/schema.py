from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..utils import str32, str64, str128, str512

# Defining the tables


class RSSBase(DeclarativeBase):
    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str512: String(512),
    }


class ElementStatusBase:
    """Prototype for tables."""

    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"}

    name: Mapped[str64] = mapped_column("Name", primary_key=True)
    statustype: Mapped[str128] = mapped_column(
        "StatusType", server_default="all", primary_key=True
    )
    vo: Mapped[str64] = mapped_column("VO", primary_key=True, server_default="all")
    status: Mapped[str] = mapped_column("Status", String(8), server_default="")
    reason: Mapped[str512] = mapped_column("Reason", server_default="Unspecified")
    dateeffective: Mapped[datetime] = mapped_column("DateEffective", DateTime)
    tokenexpiration: Mapped[datetime] = mapped_column(
        "TokenExpiration", DateTime, server_default="9999-12-31 23:59:59"
    )
    elementtype: Mapped[str32] = mapped_column("ElementType", server_default="")
    lastchecktime: Mapped[datetime] = mapped_column(
        "LastCheckTime", DateTime, server_default="1000-01-01 00:00:00"
    )
    tokenowner: Mapped[str] = mapped_column(
        "TokenOwner", String(16), server_default="rs_svc"
    )


class ElementStatusBaseWithID(ElementStatusBase):
    """Prototype for tables.

    This is almost the same as ElementStatusBase, with the following differences:
    - there's an autoincrement ID column which is also the primary key
    - the name and statusType components are not part of the primary key
    """

    id: Mapped[int] = mapped_column(
        "ID", BigInteger, autoincrement=True, primary_key=True
    )
    name: Mapped[str64] = mapped_column("Name")
    statustype: Mapped[str128] = mapped_column("StatusType", server_default="all")
    vo: Mapped[str64] = mapped_column("VO", server_default="all")
    status: Mapped[str] = mapped_column("Status", String(8), server_default="")
    reason: Mapped[str512] = mapped_column("Reason", server_default="Unspecified")
    dateeffective: Mapped[datetime] = mapped_column("DateEffective", DateTime)
    tokenexpiration: Mapped[datetime] = mapped_column(
        "TokenExpiration", DateTime, server_default="9999-12-31 23:59:59"
    )
    elementtype: Mapped[str32] = mapped_column("ElementType", server_default="")
    lastchecktime: Mapped[datetime] = mapped_column(
        "LastCheckTime", DateTime, server_default="1000-01-01 00:00:00"
    )
    tokenowner: Mapped[str] = mapped_column(
        "TokenOwner", String(16), server_default="rs_svc"
    )


# tables with schema defined in ElementStatusBase


class SiteStatus(ElementStatusBase, RSSBase):
    __tablename__ = "SiteStatus"


class ResourceStatus(ElementStatusBase, RSSBase):
    __tablename__ = "ResourceStatus"


class NodeStatus(ElementStatusBase, RSSBase):
    __tablename__ = "NodeStatus"


# tables with schema defined in ElementStatusBaseWithID


class SiteLog(ElementStatusBaseWithID, RSSBase):
    __tablename__ = "SiteLog"


class SiteHistory(ElementStatusBaseWithID, RSSBase):
    __tablename__ = "SiteHistory"


class ResourceLog(ElementStatusBaseWithID, RSSBase):
    __tablename__ = "ResourceLog"


class ResourceHistory(ElementStatusBaseWithID, RSSBase):
    __tablename__ = "ResourceHistory"


class NodeLog(ElementStatusBaseWithID, RSSBase):
    __tablename__ = "NodeLog"


class NodeHistory(ElementStatusBaseWithID, RSSBase):
    __tablename__ = "NodeHistory"
