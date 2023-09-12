# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
import asyncio
import json
from types import TracebackType
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional, cast
from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.pipeline import PipelineRequest
from azure.core.pipeline.policies import AsyncBearerTokenCredentialPolicy

from diracx.client.models import DeviceFlowErrorResponse, TokenResponse
from diracx.core.preferences import get_diracx_preferences

from ._client import Dirac as DiracGenerated

__all__: List[str] = [
    "DiracClient",
]  # Add all objects you want publicly available to users at this package level


EXPIRES_GRACE_SECONDS = 15


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


class DiracTokenCredential(AsyncTokenCredential):
    """Tailor get_token() for our context"""

    def __init__(self, location: Path, token_endpoint: str, client_id: str) -> None:
        self.location = location
        self.token_endpoint = token_endpoint
        self.client_id = client_id

    async def get_token(
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
        response = requests.post(
            url=self.token_endpoint,
            data={
                "client_id": self.client_id,
                "grant_type": "refresh_token",
                "refresh_token": kwargs.get("refresh_token"),
            },
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"An issue occured while refreshing your access token: {response.json()['detail']}"
            )

        res = response.json()
        token_response = TokenResponse(
            access_token=res["access_token"],
            expires_in=res["expires_in"],
            state=res["state"],
            refresh_token=res.get("refresh_token"),
            token_type=res.get("token_type"),
        )

        write_credentials(token_response)
        credentials = json.loads(self.location.read_text())
        return AccessToken(
            credentials.get("access_token"), credentials.get("expires_on")
        )

    async def close(self) -> None:
        """AsyncTokenCredential is a protocol: we need to 'implement' close()"""
        pass

    async def __aenter__(self):
        """AsyncTokenCredential is a protocol: we need to 'implement' __aenter__()"""
        pass

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: TracebackType | None = ...,
    ) -> None:
        """AsyncTokenCredential is a protocol: we need to 'implement' __aexit__()"""
        pass


class DiracBearerTokenCredentialPolicy(AsyncBearerTokenCredentialPolicy):
    """Custom AsyncBearerTokenCredentialPolicy tailored for our use case.

    * It does not ensure the connection is done through https.
    * It does not ensure that an access token is available.
    """

    def __init__(
        self, credential: DiracTokenCredential, *scopes: str, **kwargs: Any
    ) -> None:
        super().__init__(credential, *scopes, **kwargs)

    async def on_request(
        self, request: "PipelineRequest"
    ) -> None:  # pylint:disable=invalid-overridden-method
        """Authorization Bearer is optional here.
        :param request: The pipeline request object to be modified.
        :type request: ~azure.core.pipeline.PipelineRequest
        :raises: :class:`~azure.core.exceptions.ServiceRequestError`
        """
        self._token: AccessToken
        self._credential: DiracTokenCredential
        credentials: dict[str, Any]

        # If the credentials path does not exist, we suppose it is not needed to perform the request
        if not self._credential.location.exists():
            return

        # Load the existing credentials
        if not self._token:
            credentials = json.loads(self._credential.location.read_text())
            self._token = AccessToken(
                cast(str, credentials.get("access_token")),
                cast(int, credentials.get("expires_on")),
            )

        # Else we check if we need a new access token
        if self._need_new_token():
            if not credentials:
                credentials = json.loads(self._credential.location.read_text())
            self._token = await self._credential.get_token(
                "", refresh_token=credentials["refresh_token"]
            )

        request.http_request.headers["Authorization"] = f"Bearer {self._token.token}"

    def _need_new_token(self) -> bool:
        return (
            not self._token
            or (
                datetime.utcfromtimestamp(self._token.expires_on) - datetime.utcnow()
            ).total_seconds()
            < 300
        )


class DiracClient(DiracGenerated):
    """This class inherits from the generated Dirac client and adds support for tokens,
    so that the caller does not need to configure it by itself.
    """

    def __init__(
        self, endpoint: str | None = None, client_id: str | None = None, **kwargs: Any
    ) -> None:
        diracx_preferences = get_diracx_preferences()
        self._credentials_path = diracx_preferences.credentials_path

        self._endpoint = endpoint or diracx_preferences.url
        self._client_id = client_id or "myDIRACClientID"

        # Get .well-known configuration
        response = requests.get(
            url=f"{self._endpoint}/.well-known/openid-configuration"
        )
        if not response.ok:
            print(response.__dict__)
            raise RuntimeError(
                "Cannot fetch any information from the .well-known endpoint"
            )

        # Initialize Dirac with a Dirac-specific token credential policy
        super().__init__(
            endpoint=self._endpoint,
            authentication_policy=DiracBearerTokenCredentialPolicy(
                DiracTokenCredential(
                    location=self._credentials_path,
                    token_endpoint=response.json()["token_endpoint"],
                    client_id=self._client_id,
                ),
            ),
            **kwargs,
        )

    async def login(self, scopes: list[str]):
        """Initiate a device_code flow with a given vo and scopes."""
        data = await self.auth.initiate_device_flow(
            client_id=self._client_id,
            audience="Dirac server",
            scope=" ".join(scopes),
        )
        print("Now go to:", data.verification_uri_complete)
        expires = datetime.now() + timedelta(seconds=data.expires_in - 30)
        while expires > datetime.now():
            print(".", end="", flush=True)
            response = await self.auth.token(device_code=data.device_code, client_id="myDIRACClientID")  # type: ignore
            if isinstance(response, DeviceFlowErrorResponse):
                if response.error == "authorization_pending":
                    # TODO: Setting more than 5 seconds results in an error
                    # Related to keep-alive disconnects from uvicon (--timeout-keep-alive)
                    await asyncio.sleep(2)
                    continue
                raise RuntimeError(f"Device flow failed with {response}")
            break
        else:
            raise RuntimeError("Device authorization flow expired")

        # Save credentials
        self._credentials_path.parent.mkdir(parents=True, exist_ok=True)
        write_credentials(response)
        print(f"Saved credentials to {self._credentials_path}")

    async def logout(self):
        """Remove credentials"""
        if not self._credentials_path.exists():
            return
        credentials = json.loads(self._credentials_path.read_text())

        # Revoke refresh token
        try:
            await self.auth.revoke_refresh_token(credentials["refresh_token"])
        except Exception:
            pass

        # Remove credentials
        self._credentials_path.unlink(missing_ok=True)
        print(f"Removed credentials from {self._credentials_path}")

    async def __aenter__(self) -> "DiracClient":
        """Redefined to provide the patched Dirac client in the managed context"""
        await self._client.__aenter__()
        return self


def write_credentials(token_response: TokenResponse):
    """Write credentials received in CREDENTIALS_PATH"""
    expires = datetime.now(tz=timezone.utc) + timedelta(
        seconds=token_response.expires_in - EXPIRES_GRACE_SECONDS
    )
    credential_data = {
        "access_token": token_response.access_token,
        "refresh_token": token_response.refresh_token,
        "expires_on": int(datetime.timestamp(expires)),
    }
    get_diracx_preferences().credentials_path.write_text(json.dumps(credential_data))
