# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
from __future__ import annotations

import abc
from importlib.metadata import PackageNotFoundError, distribution
from types import TracebackType
from pathlib import Path

from typing import Any, List, Optional, cast

from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.pipeline import PipelineRequest
from azure.core.pipeline.policies import AsyncBearerTokenCredentialPolicy

from diracx.core.preferences import get_diracx_preferences, DiracxPreferences

from ..utils import (
    get_openid_configuration,
    get_token,
)

__all__: List[str] = [
    "DiracClient",
]  # Add all objects you want publicly available to users at this package level


class DiracTokenCredential(AsyncTokenCredential):
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

    async def get_token(
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

    # Make mypy happy
    _token: Optional[AccessToken] = None

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
        # Make mypy happy
        if not isinstance(self._credential, AsyncTokenCredential):
            return

        self._token = await self._credential.get_token("", token=self._token)
        if not self._token.token:
            # If we are here, it means the token is not available
            # we suppose it is not needed to perform the request
            return

        request.http_request.headers["Authorization"] = (
            "Bearer " + cast(AccessToken, self._token).token
        )


class DiracClientMixin(metaclass=abc.ABCMeta):
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
        if verify is True and diracx_preferences.ca_path:
            verify = str(diracx_preferences.ca_path)
        kwargs["connection_verify"] = verify
        self._endpoint = str(endpoint or diracx_preferences.url)
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
