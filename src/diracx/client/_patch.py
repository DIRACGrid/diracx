# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
from __future__ import annotations

from datetime import datetime
import json
import requests

from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from azure.core.credentials import AccessToken
from azure.core.credentials import TokenCredential
from azure.core.pipeline import PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy

from diracx.client.models import TokenResponse
from diracx.core.models import TokenResponse as CoreTokenResponse
from diracx.core.preferences import DiracxPreferences, get_diracx_preferences

from ._client import Dirac as DiracGenerated


__all__: List[str] = [
    "DiracClient",
]  # Add all objects you want publicly available to users at this package level


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


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
        """Refresh the access token using the refresh_token flow.
        :param str scopes: The type of access needed.
        :keyword str claims: Additional claims required in the token, such as those returned in a resource
            provider's claims challenge following an authorization failure.
        :keyword str tenant_id: Optional tenant to include in the token request.
        :rtype: AccessToken
        :return: An AccessToken instance containing the token string and its expiration time in Unix time.
        """
        return refresh_token(
            self.location,
            self.token_endpoint,
            self.client_id,
            kwargs["refresh_token"],
            verify=self.verify,
        )


class DiracBearerTokenCredentialPolicy(BearerTokenCredentialPolicy):
    """Custom BearerTokenCredentialPolicy tailored for our use case.

    * It does not ensure the connection is done through https.
    * It does not ensure that an access token is available.
    """

    def __init__(
        self, credential: DiracTokenCredential, *scopes: str, **kwargs: Any
    ) -> None:
        super().__init__(credential, *scopes, **kwargs)

    def on_request(
        self, request: PipelineRequest
    ) -> None:  # pylint:disable=invalid-overridden-method
        """Authorization Bearer is optional here.
        :param request: The pipeline request object to be modified.
        :type request: ~azure.core.pipeline.PipelineRequest
        :raises: :class:`~azure.core.exceptions.ServiceRequestError`
        """
        self._token: AccessToken | None
        self._credential: DiracTokenCredential
        credentials: dict[str, Any]

        try:
            self._token = get_token(self._credential.location, self._token)
        except RuntimeError:
            # If we are here, it means the credentials path does not exist
            # we suppose it is not needed to perform the request
            return

        if not self._token:
            credentials = json.loads(self._credential.location.read_text())
            self._token = self._credential.get_token(
                "", refresh_token=credentials["refresh_token"]
            )

        request.http_request.headers["Authorization"] = f"Bearer {self._token.token}"


class DiracClient(DiracGenerated):
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
        self._endpoint = endpoint or diracx_preferences.url
        if verify is True and diracx_preferences.ca_path:
            verify = str(diracx_preferences.ca_path)
        kwargs["connection_verify"] = verify
        self._client_id = client_id or "myDIRACClientID"

        # Get .well-known configuration
        openid_configuration = get_openid_configuration(self._endpoint, verify=verify)

        # Initialize Dirac with a Dirac-specific token credential policy
        super().__init__(
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

    def __aenter__(self) -> "DiracClient":
        """Redefined to provide the patched Dirac client in the managed context"""
        self._client.__enter__()
        return self


def refresh_token(
    location: Path,
    token_endpoint: str,
    client_id: str,
    refresh_token: str,
    *,
    verify: bool | str = True,
) -> AccessToken:
    """Refresh the access token using the refresh_token flow."""
    from diracx.core.utils import write_credentials

    response = requests.post(
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
    token_response = TokenResponse(
        access_token=res["access_token"],
        expires_in=res["expires_in"],
        token_type=res.get("token_type"),
        refresh_token=res.get("refresh_token"),
    )

    write_credentials(cast(CoreTokenResponse, token_response), location=location)
    credentials = json.loads(location.read_text())
    return AccessToken(credentials.get("access_token"), credentials.get("expires_on"))


def get_openid_configuration(
    endpoint: str, *, verify: bool | str = True
) -> Dict[str, str]:
    """Get the openid configuration from the .well-known endpoint"""
    response = requests.get(
        url=f"{endpoint}/.well-known/openid-configuration",
        verify=verify,
    )
    if not response.ok:
        raise RuntimeError("Cannot fetch any information from the .well-known endpoint")
    return response.json()


def get_token(location: Path, token: AccessToken | None) -> AccessToken | None:
    """Get token if available and still valid."""
    # If the credentials path does not exist, raise an error
    if not location.exists():
        raise RuntimeError("credentials are not set")

    # Load the existing credentials
    if not token:
        credentials = json.loads(location.read_text())
        token = AccessToken(
            cast(str, credentials.get("access_token")),
            cast(int, credentials.get("expires_on")),
        )

    # We check the validity of the token
    # If not valid, then return None to inform the caller that a new token
    # is needed
    if not is_token_valid(token):
        return None

    return token


def is_token_valid(token: AccessToken) -> bool:
    """Condition to get a new token"""
    return (
        datetime.utcfromtimestamp(token.expires_on) - datetime.utcnow()
    ).total_seconds() > 300
