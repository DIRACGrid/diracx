from enum import Enum, auto

from sqlalchemy import (
    JSON,
    String,
    Uuid,
)
from sqlalchemy.orm import declarative_base

from diracx.db.sql.utils import Column, DateNowColumn, EnumColumn, NullColumn

USER_CODE_LENGTH = 8

Base = declarative_base()


class FlowStatus(Enum):
    """
    The normal flow is
    PENDING -> READY -> DONE
    Pending is upon insertion
    Ready/Error is set in response to IdP
    Done means the user has been issued the dirac token
    """

    # The flow is ongoing
    PENDING = auto()
    # The user has been authenticated
    # and we are ready to issue a token
    READY = auto()
    # The token has been minted,
    # the user can no longer login with this flow
    DONE = auto()

    # Something went wrong.
    # Most likely the user did not login in the
    # external IdP
    ERROR = auto()


class DeviceFlows(Base):
    __tablename__ = "DeviceFlows"
    user_code = Column(String(USER_CODE_LENGTH), primary_key=True)
    status = EnumColumn(FlowStatus, server_default=FlowStatus.PENDING.name)
    creation_time = DateNowColumn()
    client_id = Column(String(255))
    scope = Column(String(1024))
    audience = Column(String(255))
    device_code = Column(String(128), unique=True)  # hash it ?
    id_token = NullColumn(JSON())


class AuthorizationFlows(Base):
    __tablename__ = "AuthorizationFlows"
    uuid = Column(Uuid(as_uuid=False), primary_key=True)
    status = EnumColumn(FlowStatus, server_default=FlowStatus.PENDING.name)
    client_id = Column(String(255))
    creation_time = DateNowColumn()
    scope = Column(String(1024))
    audience = Column(String(255))
    code_challenge = Column(String(255))
    code_challenge_method = Column(String(8))
    redirect_uri = Column(String(255))
    code = NullColumn(String(255))  # hash it ?
    id_token = NullColumn(JSON())


class RefreshTokenStatus(Enum):
    """
    The normal flow is
    CREATED -> REVOKED

    Note1: There is no EXPIRED status as it can be calculated from a creation time
    Note2: As part of the refresh token rotation mechanism, the revoked token should be retained
    """

    # The token has been created
    # It does not indicate whether the token is valid or not
    CREATED = auto()

    # The token has been revoked
    REVOKED = auto()


class RefreshTokens(Base):
    """Store attributes bound to a refresh token, as well as specific user attributes
    that might be then used to generate access tokens
    """

    __tablename__ = "RefreshTokens"
    # Refresh token attributes
    jti = Column(Uuid(as_uuid=False), primary_key=True)
    status = EnumColumn(
        RefreshTokenStatus, server_default=RefreshTokenStatus.CREATED.name
    )
    creation_time = DateNowColumn()
    scope = Column(String(1024))

    # User attributes bound to the refresh token
    sub = Column(String(1024))
    preferred_username = Column(String(255))
