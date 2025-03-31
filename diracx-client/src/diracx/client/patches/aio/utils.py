# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
from __future__ import annotations

import abc
import io
import json
from importlib.metadata import PackageNotFoundError, distribution
from types import TracebackType
from pathlib import Path

from typing import Any, Dict, List, Optional, cast

from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.exceptions import map_error, HttpResponseError
from azure.core.pipeline import PipelineRequest, PipelineResponse
from azure.core.pipeline.policies import AsyncBearerTokenCredentialPolicy
from azure.core.tracing.decorator_async import distributed_trace_async

from diracx.client.generated.aio.operations._operations import (
    AuthOperations as AuthOperationsGenerated,
    JobsOperations as JobsOperationsGenerated,
)
from diracx.client.generated.models._models import (
    TokenResponse as TokenResponseGenerated,
)


from diracx.core.preferences import get_diracx_preferences, DiracxPreferences

from ..utils import (
    build_token_request,
    get_openid_configuration,
    get_token,
    DeviceFlowErrorResponse,
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


class AuthOperations(AuthOperationsGenerated):
    @distributed_trace_async
    async def get_oidc_token(
        self, device_code: str, client_id: str, **kwargs
    ) -> TokenResponseGenerated | DeviceFlowErrorResponse:
        request = build_token_request(
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": device_code,
                "client_id": client_id,
            },
        )
        request.url = self._client.format_url(request.url)

        _stream = False
        pipeline_response: PipelineResponse = (
            await self._client._pipeline.run(  # pylint: disable=protected-access
                request, stream=_stream, **kwargs
            )
        )

        response = pipeline_response.http_response

        if response.status_code == 200:
            return self._deserialize("TokenResponse", pipeline_response)
        elif response.status_code == 400:
            return self._deserialize("DeviceFlowErrorResponse", pipeline_response)
        else:
            map_error(status_code=response.status_code, response=response, error_map={})
            raise HttpResponseError(response=response)


class JobsOperations(JobsOperationsGenerated):
    @distributed_trace_async
    async def search(  # type: ignore[override]
        self,
        *,
        parameters: list[str] | None = None,
        search: list[str] | None = None,
        sort: list[str] | None = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """TODO"""
        body = {}
        if parameters is not None:
            body["parameters"] = parameters
        if search is not None:
            body["search"] = search
        if sort is not None:
            body["sort"] = sort
        # TODO: The BytesIO here is only needed to satify the typing
        # Probably an autorest bug
        body_data = io.BytesIO(json.dumps(body).encode("utf-8"))
        return await super().search(body_data, **kwargs)

    @distributed_trace_async
    async def summary(  # type: ignore[override]
        self,
        *,
        grouping: list[str] | None = None,
        search: list[str] | None = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """TODO"""
        body = {}
        if grouping is not None:
            body["grouping"] = grouping
        if search is not None:
            body["search"] = search
        # TODO: The BytesIO here is only needed to satify the typing
        # Probably an autorest bug
        body_data = io.BytesIO(json.dumps(body).encode("utf-8"))
        return await super().summary(body_data, **kwargs)
