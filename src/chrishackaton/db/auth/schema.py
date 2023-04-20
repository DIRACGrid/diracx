from sqlalchemy import (
    JSON,
    String,
    Uuid,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, DateNowColumn, NullColumn

USER_CODE_LENGTH = 8

Base = declarative_base()


class DeviceFlows(Base):
    __tablename__ = "DeviceFlows"
    user_code = Column(String(USER_CODE_LENGTH), primary_key=True)
    creation_time = DateNowColumn()
    client_id = Column(String(255))
    scope = Column(String(1024))
    audience = Column(String(255))
    device_code = Column(String(128), unique=True)  # hash it ?
    id_token = NullColumn(JSON())


class AuthorizationFlows(Base):
    __tablename__ = "AuthorizationFlows"
    uuid = Column(Uuid(), primary_key=True)
    client_id = Column(String(255))
    creation_time = DateNowColumn()
    scope = Column(String(1024))
    audience = Column(String(255))
    code_challenge = Column(String(255))
    redirect_uri = Column(String(255))
    code = Column(String(255))  # hash it ?
    id_token = NullColumn(JSON())
