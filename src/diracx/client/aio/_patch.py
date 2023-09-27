# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
import json
import logging
from types import TracebackType
from pathlib import Path
from typing import Any, List, Optional
from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.pipeline import PipelineRequest
from azure.core.pipeline.policies import AsyncBearerTokenCredentialPolicy

from diracx.core.preferences import get_diracx_preferences

from ._client import Dirac as DiracGenerated
from .._patch import get_openid_configuration, get_token, refresh_token

__all__: List[str] = [
    "DiracClient",
]  # Add all objects you want publicly available to users at this package level

logger = logging.getLogger(__name__)


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
    ) -> AccessToken | None:
        """Refresh the access token using the refresh_token flow.
        :param str scopes: The type of access needed.
        :keyword str claims: Additional claims required in the token, such as those returned in a resource
            provider's claims challenge following an authorization failure.
        :keyword str tenant_id: Optional tenant to include in the token request.
        :rtype: AccessToken
        :return: An AccessToken instance containing the token string and its expiration time in Unix time.
        """
        return refresh_token(
            self.location, self.token_endpoint, self.client_id, kwargs["refresh_token"]
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
            # TODO: Use httpx and await this call
            self._token = get_token(self._credential.location, self._token)
        except RuntimeError:
            # If we are here, it means the credentials path does not exist
            # we suppose it is not needed to perform the request
            return

        if not self._token:
            try:
                credentials = json.loads(self._credential.location.read_text())
            except Exception:
                logger.warning(
                    "Cannot load credentials from %s", self._credential.location
                )
            else:
                self._token = await self._credential.get_token(
                    "", refresh_token=credentials["refresh_token"]
                )

        if self._token:
            request.http_request.headers[
                "Authorization"
            ] = f"Bearer {self._token.token}"


class DiracClient(DiracGenerated):
    """This class inherits from the generated Dirac client and adds support for tokens,
    so that the caller does not need to configure it by itself.
    """

    def __init__(
        self, endpoint: str | None = None, client_id: str | None = None, **kwargs: Any
    ) -> None:
        diracx_preferences = get_diracx_preferences()
        self._endpoint = endpoint or diracx_preferences.url
        self._client_id = client_id or "myDIRACClientID"

        # Get .well-known configuration
        openid_configuration = get_openid_configuration(self._endpoint)

        # Initialize Dirac with a Dirac-specific token credential policy
        super().__init__(
            endpoint=self._endpoint,
            authentication_policy=DiracBearerTokenCredentialPolicy(
                DiracTokenCredential(
                    location=diracx_preferences.credentials_path,
                    token_endpoint=openid_configuration["token_endpoint"],
                    client_id=self._client_id,
                ),
            ),
            **kwargs,
        )

    @property
    def client_id(self):
        return self._client_id

    async def __aenter__(self) -> "DiracClient":
        """Redefined to provide the patched Dirac client in the managed context"""
        await self._client.__aenter__()
        return self
