"""Patches for the autorest-generated jobs client.

This file can be used to customize the generated code for the jobs client.
When adding new classes to this file, make sure to also add them to the
__all__ list in the corresponding file in the patches directory.
"""

from __future__ import annotations

__all__ = [
    "AuthOperations",
]

from azure.core.pipeline import PipelineResponse
from azure.core.tracing.decorator import distributed_trace

from ..._generated.operations._operations import (
    _models,
    AuthOperations as _AuthOperations,
)
from .common import handle_revoke_response, prepare_oidc_request, handle_oidc_response, prepare_revoke_request


class AuthOperations(_AuthOperations):
    @distributed_trace
    def get_oidc_token(
        self, device_code: str, client_id: str, **kwargs
    ) -> _models.TokenResponse | _models.DeviceFlowErrorResponse:
        request = prepare_oidc_request(
            device_code=device_code,
            client_id=client_id,
            format_url=self._client.format_url,
        )

        pipeline_response: PipelineResponse = (
            self._client._pipeline.run(  # pylint: disable=protected-access
                request, stream=False, **kwargs
            )
        )

        return handle_oidc_response(pipeline_response, self._deserialize)


    @distributed_trace
    def revoke_refresh_token_by_refresh_token(
        self,
        *,
        token: str,
        client_id: str,
        token_type_hint: str = "refresh_token",
        **kwargs,
    ) -> str:
        request = prepare_revoke_request(
            token=token,
            client_id=client_id,
            token_type_hint=token_type_hint,
            format_url=self._client.format_url,
        )

        pipeline_response: PipelineResponse = (
            self._client._pipeline.run(  # pylint: disable=protected-access
                request, stream=False, **kwargs
            )
        )

        return handle_revoke_response(pipeline_response, self._deserialize)
