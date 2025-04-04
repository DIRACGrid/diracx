"""Utilities which are common to the sync and async auth operator patches."""

from __future__ import annotations

__all__ = [
    "prepare_request",
    "handle_response",
]

from typing import Any

from azure.core.exceptions import map_error, HttpResponseError
from azure.core.pipeline import PipelineResponse
from azure.core.rest import HttpRequest
from azure.core.utils import case_insensitive_dict

from ..._generated.models import TokenResponse, DeviceFlowErrorResponse
from ..._generated.operations._operations import _SERIALIZER


def build_token_request(**kwargs: Any) -> HttpRequest:
    _headers = case_insensitive_dict(kwargs.pop("headers", {}) or {})

    accept = _headers.pop("Accept", "application/json")

    _url = "/api/auth/token"

    _headers["Accept"] = _SERIALIZER.header("accept", accept, "str")

    return HttpRequest(method="POST", url=_url, headers=_headers, **kwargs)


def prepare_request(device_code, client_id, format_url) -> HttpRequest:
    request = build_token_request(
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": device_code,
            "client_id": client_id,
        },
    )
    request.url = format_url(request.url)
    return request


def handle_response(
    pipeline_response: PipelineResponse, deserialize
) -> TokenResponse | DeviceFlowErrorResponse:
    response = pipeline_response.http_response

    if response.status_code == 200:
        return deserialize("TokenResponse", pipeline_response)
    elif response.status_code == 400:
        return deserialize("DeviceFlowErrorResponse", pipeline_response)
    else:
        map_error(status_code=response.status_code, response=response, error_map={})
        raise HttpResponseError(response=response)
