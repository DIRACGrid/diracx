"""SQLAlchemy table schemas for authentication flows and refresh tokens."""

from __future__ import annotations

from enum import Enum, auto
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import (
    JSON,
    Index,
    String,
    Uuid,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils import (
    datetime_now,
    enum_column,
    str128,
    str255,
    str1024,
)

USER_CODE_LENGTH = 8


class Base(DeclarativeBase):
    """Declarative base with shared SQLAlchemy type mappings.

    Attributes:
        type_annotation_map: SQL types associated with shared Python aliases.
    """

    type_annotation_map = {
        str128: String(128),
        str255: String(255),
        str1024: String(1024),
        dict[str, Any]: JSON,
    }


class FlowStatus(Enum):
    """PENDING -> READY -> DONE.

    Pending is upon insertion
    Ready/Error is set in response to IdP
    Done means the user has been issued the dirac token.
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
    """Database record for an OAuth device authorization flow.

    Attributes:
        user_code: Short code entered by the user to approve the flow.
        status: Current state of the device flow.
        creation_time: Time when the flow was created.
        client_id: OAuth client that initiated the flow.
        scope: Requested authorization scope.
        device_code: Hashed device code used to identify the flow.
        id_token: ID token claims received from the identity provider, if any.
    """

    __tablename__ = "DeviceFlows"
    user_code: Mapped[str] = mapped_column(
        "UserCode", String(USER_CODE_LENGTH), primary_key=True
    )
    status: Mapped[FlowStatus] = enum_column(
        "Status", FlowStatus, server_default=FlowStatus.PENDING.name
    )
    creation_time: Mapped[datetime_now] = mapped_column("CreationTime")
    client_id: Mapped[str255] = mapped_column("ClientID")
    scope: Mapped[str1024] = mapped_column("Scope")
    device_code: Mapped[str128] = mapped_column(
        "DeviceCode", unique=True
    )  # Should be a hash
    id_token: Mapped[Optional[dict[str, Any]]] = mapped_column("IDToken")


class AuthorizationFlows(Base):
    """Database record for an OAuth authorization-code flow.

    Attributes:
        uuid: Identifier of the authorization flow.
        status: Current state of the authorization flow.
        client_id: OAuth client that initiated the flow.
        creation_time: Time when the flow was created.
        scope: Requested authorization scope.
        code_challenge: PKCE code challenge for the flow.
        code_challenge_method: Method used to derive the challenge.
        redirect_uri: Client URI for the authorization response.
        code: Hashed authorization code, if the flow is ready.
        id_token: ID token claims received from the identity provider, if any.
    """

    __tablename__ = "AuthorizationFlows"
    uuid: Mapped[UUID] = mapped_column("UUID", Uuid(as_uuid=False), primary_key=True)
    status: Mapped[FlowStatus] = enum_column(
        "Status", FlowStatus, server_default=FlowStatus.PENDING.name
    )
    client_id: Mapped[str255] = mapped_column("ClientID")
    creation_time: Mapped[datetime_now] = mapped_column("CreationTime")
    scope: Mapped[str1024] = mapped_column("Scope")
    code_challenge: Mapped[str255] = mapped_column("CodeChallenge")
    code_challenge_method: Mapped[str] = mapped_column("CodeChallengeMethod", String(8))
    redirect_uri: Mapped[str255] = mapped_column("RedirectURI")
    code: Mapped[Optional[str255]] = mapped_column("Code")  # Should be a hash
    id_token: Mapped[Optional[dict[str, Any]]] = mapped_column("IDToken")


class RefreshTokenStatus(Enum):
    """CREATED -> REVOKED.

    Note1: There is no EXPIRED status as it can be calculated from a creation time
    Note2: As part of the refresh token rotation mechanism, the revoked token should be retained
    """

    # The token has been created
    # It does not indicate whether the token is valid or not
    CREATED = auto()

    # The token has been revoked
    REVOKED = auto()


class RefreshTokens(Base):
    """Store attributes bound to a refresh token.

    Also specific user attributes that might be then used to generate access tokens.

    Attributes:
        jti: Unique JWT identifier for the refresh token.
        status: Current refresh token status.
        scope: Authorized scope associated with the token.
        sub: Subject identifier associated with the token.
    """

    __tablename__ = "RefreshTokens"
    # Refresh token attributes
    jti: Mapped[UUID] = mapped_column("JTI", Uuid(as_uuid=False), primary_key=True)
    status: Mapped[RefreshTokenStatus] = enum_column(
        "Status", RefreshTokenStatus, server_default=RefreshTokenStatus.CREATED.name
    )
    scope: Mapped[str1024] = mapped_column("Scope")

    # User attributes bound to the refresh token
    sub: Mapped[str] = mapped_column("Sub", String(256), index=True)

    __table_args__ = (Index("index_status_sub", status, sub),)
