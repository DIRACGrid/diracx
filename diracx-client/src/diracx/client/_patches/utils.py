from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import fcntl
import json
import os
from diracx.core.utils import EXPIRES_GRACE_SECONDS, serialize_credentials
import httpx
import jwt

from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, distribution

from pathlib import Path

from typing import Any, Dict, Optional, TextIO
from urllib import parse
from azure.core.credentials import AccessToken
from azure.core.credentials import TokenCredential
from azure.core.pipeline import PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy

from diracx.core.models import TokenResponse
from diracx.core.preferences import DiracxPreferences, get_diracx_preferences


class TokenStatus(Enum):
    VALID = "valid"
    REFRESH = "refresh"
    INVALID = "invalid"


@dataclass
class TokenResult:
    status: TokenStatus
    access_token: Optional[AccessToken] = None
    refresh_token: Optional[str] = None


def get_openid_configuration(
    endpoint: str, *, verify: bool | str = True
) -> Dict[str, str]:
    """Get the openid configuration from the .well-known endpoint"""
    response = httpx.get(
        url=parse.urljoin(endpoint, ".well-known/openid-configuration"),
        verify=verify,
    )
    if not response.is_success:
        raise RuntimeError("Cannot fetch any information from the .well-known endpoint")
    return response.json()


def get_token(
    location: Path,
    token: AccessToken | None,
    token_endpoint: str,
    client_id: str,
    verify: bool | str,
) -> AccessToken:
    """Get the access token if available and still valid."""
    # Immediately return the token if it is available and still valid
    if token and is_token_valid(token):
        return token

    if not location.exists():
        # If we are here, it means the credentials path does not exist
        # we suppose access token is not needed to perform the request
        # we return an empty token to align with the expected return type
        return AccessToken(token="", expires_on=0)

    with open(location, "r+") as f:
        # Acquire exclusive lock
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            response = extract_token_from_credentials(f, token)
            if response.status == TokenStatus.VALID and response.access_token:
                # Lock is released in the finally block
                return response.access_token

            if response.status == TokenStatus.REFRESH and response.refresh_token:
                # If we are here, it means the token needs to be refreshed
                token_response = refresh_token(
                    token_endpoint,
                    client_id,
                    response.refresh_token,
                    verify=verify,
                )

                # Write the new credentials to the file
                f.seek(0)
                f.truncate()
                f.write(serialize_credentials(token_response))
                f.flush()
                os.fsync(f.fileno())

                # Get an AccessToken instance
                return AccessToken(
                    token=token_response.access_token,
                    expires_on=int(
                        (
                            datetime.now(tz=timezone.utc)
                            + timedelta(
                                seconds=token_response.expires_in
                                - EXPIRES_GRACE_SECONDS
                            )
                        ).timestamp()
                    ),
                )
            # If we are here, it means the token is not available or not valid anymore
            return AccessToken(token="", expires_on=0)
        finally:
            # Release the lock
            fcntl.flock(f, fcntl.LOCK_UN)


def refresh_token(
    token_endpoint: str,
    client_id: str,
    refresh_token: str,
    *,
    verify: bool | str = True,
) -> TokenResponse:
    """Refresh the access token using the refresh_token flow."""
    response = httpx.post(
        url=token_endpoint,
        data={
            "client_id": client_id,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        verify=verify,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"An issue occured while refreshing your access token: {response.json()['detail']}"
        )

    res = response.json()
    return TokenResponse(
        access_token=res["access_token"],
        expires_in=res["expires_in"],
        token_type=res.get("token_type"),
        refresh_token=res.get("refresh_token"),
    )


def extract_token_from_credentials(
    token_file_descriptor: TextIO, token: AccessToken | None
) -> TokenResult:
    """Get token if available and still valid."""
    # If we are here, it means the token is not available or not valid anymore
    # We try to get it from the file
    try:
        credentials = json.load(token_file_descriptor)
    except json.JSONDecodeError:
        return TokenResult(TokenStatus.INVALID)

    try:
        token = AccessToken(
            token=credentials["access_token"],
            expires_on=credentials["expires_on"],
        )
        refresh_token = credentials["refresh_token"]
    except KeyError:
        return TokenResult(TokenStatus.INVALID)

    # We check the validity of the tokens
    if is_token_valid(token):
        return TokenResult(TokenStatus.VALID, access_token=token)

    if is_refresh_token_valid(refresh_token):
        return TokenResult(TokenStatus.REFRESH, refresh_token=refresh_token)

    # If we are here, it means the refresh token is not valid anymore
    return TokenResult(TokenStatus.INVALID)


def is_refresh_token_valid(refresh_token: str | None) -> bool:
    """Check if the refresh token is still valid."""
    if not refresh_token:
        return False
    # Decode the refresh token
    refresh_payload = jwt.decode(refresh_token, options={"verify_signature": False})
    if not refresh_payload or "exp" not in refresh_payload:
        return False

    # Check the expiration time
    return refresh_payload["exp"] > datetime.now(tz=timezone.utc).timestamp()


def is_token_valid(token: AccessToken) -> bool:
    """Condition to get a new token"""
    return (
        datetime.fromtimestamp(token.expires_on, tz=timezone.utc)
        - datetime.now(tz=timezone.utc)
    ).total_seconds() > 300


class DiracTokenCredential(TokenCredential):
    """Tailor get_token() for our context"""

    def __init__(
        self,
        location: Path,
        token_endpoint: str,
        client_id: str,
        *,
        verify: bool | str = True,
    ) -> None:
        self.location = location
        self.verify = verify
        self.token_endpoint = token_endpoint
        self.client_id = client_id

    def get_token(
        self,
        *scopes: str,
        claims: Optional[str] = None,
        tenant_id: Optional[str] = None,
        **kwargs: Any,
    ) -> AccessToken:
        return get_token(
            self.location,
            kwargs.get("token"),
            self.token_endpoint,
            self.client_id,
            self.verify,
        )


class DiracBearerTokenCredentialPolicy(BearerTokenCredentialPolicy):
    """Custom BearerTokenCredentialPolicy tailored for our use case.

    * It does not ensure the connection is done through https.
    * It does not ensure that an access token is available.
    """

    # Make mypy happy
    _token: Optional[AccessToken] = None

    def __init__(
        self, credential: DiracTokenCredential, *scopes: str, **kwargs: Any
    ) -> None:
        super().__init__(credential, *scopes, **kwargs)

    def on_request(self, request: PipelineRequest) -> None:
        """Authorization Bearer is optional here.
        :param request: The pipeline request object to be modified.
        :type request: ~azure.core.pipeline.PipelineRequest
        :raises: :class:`~azure.core.exceptions.ServiceRequestError`
        """
        # Make mypy happy
        if not isinstance(self._credential, TokenCredential):
            return

        self._token = self._credential.get_token("", token=self._token)
        if not self._token.token:
            # If we are here, it means the token is not available
            # we suppose it is not needed to perform the request
            return

        self._update_headers(request.http_request.headers, self._token.token)


class DiracClientMixin:
    """This class inherits from the generated Dirac client and adds support for tokens,
    so that the caller does not need to configure it by itself.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        client_id: str | None = None,
        diracx_preferences: DiracxPreferences | None = None,
        verify: bool | str = True,
        **kwargs: Any,
    ) -> None:
        diracx_preferences = diracx_preferences or get_diracx_preferences()
        self._endpoint = str(endpoint or diracx_preferences.url)
        if verify is True and diracx_preferences.ca_path:
            verify = str(diracx_preferences.ca_path)
        kwargs["connection_verify"] = verify
        self._client_id = client_id or "myDIRACClientID"

        # Get .well-known configuration
        openid_configuration = get_openid_configuration(self._endpoint, verify=verify)

        try:
            self.client_version = distribution("diracx").version
        except PackageNotFoundError:
            try:
                self.client_version = distribution("diracx-client").version
            except PackageNotFoundError:
                print("Error while getting client version")
                self.client_version = "Unknown"

        # Setting default headers
        kwargs.setdefault("base_headers", {})[
            "DiracX-Client-Version"
        ] = self.client_version

        # Initialize Dirac with a Dirac-specific token credential policy
        # We need to ignore types here because mypy complains that we give
        # too many arguments to "object" constructor as this is a mixin

        super().__init__(  # type: ignore
            endpoint=self._endpoint,
            authentication_policy=DiracBearerTokenCredentialPolicy(
                DiracTokenCredential(
                    location=diracx_preferences.credentials_path,
                    token_endpoint=openid_configuration["token_endpoint"],
                    client_id=self._client_id,
                    verify=verify,
                ),
            ),
            **kwargs,
        )

    @property
    def client_id(self):
        return self._client_id
