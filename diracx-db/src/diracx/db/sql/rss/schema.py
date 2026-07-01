from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils.types import SmarterDateTime

from ..utils import str32, str64, str128, str512

"""RSS schema definitions.

Contains base declarative classes and table mappings used by the RSS
resource/site status subsystem.
"""


class RSSBase(DeclarativeBase):
    """Base declarative class for RSS tables.

    The :attr:`type_annotation_map` maps compact annotation aliases like
    ``str32`` to concrete SQLAlchemy ``String`` column types used throughout
    the module.
    """

    type_annotation_map = {
        str32: String(32),
        str64: String(64),
        str128: String(128),
        str512: String(512),
    }


class ElementStatusBase:
    """Shared column definitions for element status tables.

    This mixin defines the common columns used for site/resource status
    tables such as ``Name``, ``StatusType``, ``VO``, ``Status`` and timing
    fields.

    Attributes:
        name (str): Element name (primary key in some tables).
        status_type (str): Status type identifier, defaults to ``"all"``.
        vo (str): Virtual organization, defaults to ``"all"``.
        status (str): Short status string.
        reason (str): Human-readable reason.
        date_effective (datetime): When the status takes effect.
        token_expiration (datetime): Token expiration timestamp.
        element_type (str): Element type string.
        last_check_time (datetime): Last check timestamp.
        token_owner (str): Token owner identifier.
    """

    name: Mapped[str64] = mapped_column("Name", primary_key=True)
    status_type: Mapped[str128] = mapped_column(
        "StatusType", server_default="all", primary_key=True
    )
    vo: Mapped[str64] = mapped_column("VO", primary_key=True, server_default="all")
    status: Mapped[str] = mapped_column("Status", String(8), server_default="")
    reason: Mapped[str512] = mapped_column("Reason", server_default="Unspecified")
    date_effective: Mapped[datetime] = mapped_column("DateEffective", SmarterDateTime())
    token_expiration: Mapped[datetime] = mapped_column(
        "TokenExpiration", SmarterDateTime(), server_default="9999-12-31 23:59:59"
    )
    element_type: Mapped[str32] = mapped_column("ElementType", server_default="")
    last_check_time: Mapped[datetime] = mapped_column(
        "LastCheckTime", SmarterDateTime(), server_default="1000-01-01 00:00:00"
    )
    token_owner: Mapped[str] = mapped_column(
        "TokenOwner", String(16), server_default="rs_svc"
    )


class ElementStatusBaseWithID(ElementStatusBase):
    """Variant of :class:`ElementStatusBase` that uses an autoincrement ID.

    Differences from :class:`ElementStatusBase`:
        - An autoincrementing ``ID`` column is used as the primary key.
        - ``Name`` and ``StatusType`` are regular columns (not part of PK).
    """

    id: Mapped[int] = mapped_column(
        "ID", BigInteger, autoincrement=True, primary_key=True
    )
    name: Mapped[str64] = mapped_column("Name")
    status_type: Mapped[str128] = mapped_column("StatusType", server_default="all")
    vo: Mapped[str64] = mapped_column("VO", server_default="all")
    status: Mapped[str] = mapped_column("Status", String(8), server_default="")
    reason: Mapped[str512] = mapped_column("Reason", server_default="Unspecified")
    date_effective: Mapped[datetime] = mapped_column("DateEffective", SmarterDateTime())
    token_expiration: Mapped[datetime] = mapped_column(
        "TokenExpiration", SmarterDateTime(), server_default="9999-12-31 23:59:59"
    )
    element_type: Mapped[str32] = mapped_column("ElementType", server_default="")
    last_check_time: Mapped[datetime] = mapped_column(
        "LastCheckTime", SmarterDateTime(), server_default="1000-01-01 00:00:00"
    )
    token_owner: Mapped[str] = mapped_column(
        "TokenOwner", String(16), server_default="rs_svc"
    )


# Concrete tables


class SiteStatus(ElementStatusBase, RSSBase):
    """Per-site current status rows.

    Uses the columns from :class:`ElementStatusBase` and is backed by the
    ``SiteStatus`` table.
    """

    __tablename__ = "SiteStatus"


class ResourceStatus(ElementStatusBase, RSSBase):
    """Per-resource current status rows.

    Uses the columns from :class:`ElementStatusBase` and is backed by the
    ``ResourceStatus`` table.
    """

    __tablename__ = "ResourceStatus"


# Audit/history tables


class SiteLog(ElementStatusBaseWithID, RSSBase):
    """Historical site log entries with autoincrementing IDs."""

    __tablename__ = "SiteLog"


class SiteHistory(ElementStatusBaseWithID, RSSBase):
    """Site history rows (archival)."""

    __tablename__ = "SiteHistory"


class ResourceLog(ElementStatusBaseWithID, RSSBase):
    """Historical resource log entries with autoincrementing IDs."""

    __tablename__ = "ResourceLog"


class ResourceHistory(ElementStatusBaseWithID, RSSBase):
    """Resource history rows (archival)."""

    __tablename__ = "ResourceHistory"
