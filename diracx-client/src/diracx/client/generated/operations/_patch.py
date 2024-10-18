# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""

import io
import json
from typing import Any, List, Union

from azure.core.rest import HttpRequest
from azure.core.exceptions import map_error, HttpResponseError
from azure.core.utils import case_insensitive_dict
from azure.core.pipeline import PipelineResponse

from azure.core.tracing.decorator import distributed_trace

from .. import models as _models
from ._operations import (
    AuthOperations as AuthOperationsGenerated,
    JobsOperations as JobsOperationsGenerated,
    JSON,
    _SERIALIZER,
)

__all__: List[str] = [
    "AuthOperations",
    "JobsOperations",
]  # Add all objects you want publicly available to users at this package level


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


def build_token_request(vo: str, **kwargs: Any) -> HttpRequest:
    _headers = case_insensitive_dict(kwargs.pop("headers", {}) or {})

    accept = _headers.pop("Accept", "application/json")

    # Construct URL
    _url = "/api/auth/token"

    _headers["Accept"] = _SERIALIZER.header("accept", accept, "str")

    return HttpRequest(method="POST", url=_url, headers=_headers, **kwargs)


class AuthOperations(AuthOperationsGenerated):
    @distributed_trace
    async def token(
        self, vo: str, device_code: str, client_id: str, **kwargs
    ) -> _models.TokenResponse | _models.DeviceFlowErrorResponse:
        request = build_token_request(
            vo=vo,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": device_code,
                "client_id": client_id,
            },
        )
        request.url = self._client.format_url(request.url)

        _stream = False
        pipeline_response: PipelineResponse = (
            self._client._pipeline.run(  # pylint: disable=protected-access
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
    @distributed_trace
    def search(  # type: ignore[override]
        self,
        *,
        parameters: list[str] | None = None,
        search: list[str] | None = None,
        sort: list[str] | None = None,
        **kwargs: Any,
    ) -> List[JSON]:
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
        return super().search(body_data, **kwargs)

    @distributed_trace
    def summary(  # type: ignore[override]
        self,
        *,
        grouping: list[str] | None = None,
        search: list[str] | None = None,
        **kwargs: Any,
    ) -> List[JSON]:
        """TODO"""
        body = {}
        if grouping is not None:
            body["grouping"] = grouping
        if search is not None:
            body["search"] = search
        # TODO: The BytesIO here is only needed to satify the typing
        # Probably an autorest bug
        body_data = io.BytesIO(json.dumps(body).encode("utf-8"))
        return super().summary(body_data, **kwargs)
