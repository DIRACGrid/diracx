from __future__ import annotations

from sqlalchemy import (
    BLOB,
    DateTime,
    String,
)
from sqlalchemy.orm import declarative_base

from diracx.db.sql.utils import Column, NullColumn

Base = declarative_base()


class CleanProxies(Base):
    __tablename__ = "ProxyDB_CleanProxies"
    UserName = Column(String(64))
    Pem = NullColumn(BLOB)
    ProxyProvider = Column(String(64), default="Certificate")
    ExpirationTime = NullColumn(DateTime)
    UserDN = Column(String(255), primary_key=True)
