# coding=utf-8
# --------------------------------------------------------------------------
# Code generated by Microsoft (R) AutoRest Code Generator (autorest: 3.10.4, generator: @autorest/python@6.32.3)
# Changes may cause incorrect behavior and will be lost if the code is regenerated.
# --------------------------------------------------------------------------

from copy import deepcopy
from typing import Any
from typing_extensions import Self

from azure.core import PipelineClient
from azure.core.pipeline import policies
from azure.core.rest import HttpRequest, HttpResponse

from . import models as _models
from ._configuration import DiracConfiguration
from ._serialization import Deserializer, Serializer
from .operations import (
    AuthOperations,
    ConfigOperations,
    JobsOperations,
    LollygagOperations,
    PilotsOperations,
    WellKnownOperations,
)


class Dirac:  # pylint: disable=client-accepts-api-version-keyword
    """Dirac.

    :ivar well_known: WellKnownOperations operations
    :vartype well_known: _generated.operations.WellKnownOperations
    :ivar auth: AuthOperations operations
    :vartype auth: _generated.operations.AuthOperations
    :ivar config: ConfigOperations operations
    :vartype config: _generated.operations.ConfigOperations
    :ivar jobs: JobsOperations operations
    :vartype jobs: _generated.operations.JobsOperations
    :ivar lollygag: LollygagOperations operations
    :vartype lollygag: _generated.operations.LollygagOperations
    :ivar pilots: PilotsOperations operations
    :vartype pilots: _generated.operations.PilotsOperations
    :keyword endpoint: Service URL. Required. Default value is "".
    :paramtype endpoint: str
    """

    def __init__(  # pylint: disable=missing-client-constructor-parameter-credential
        self, *, endpoint: str = "", **kwargs: Any
    ) -> None:
        self._config = DiracConfiguration(**kwargs)
        _policies = kwargs.pop("policies", None)
        if _policies is None:
            _policies = [
                policies.RequestIdPolicy(**kwargs),
                self._config.headers_policy,
                self._config.user_agent_policy,
                self._config.proxy_policy,
                policies.ContentDecodePolicy(**kwargs),
                self._config.redirect_policy,
                self._config.retry_policy,
                self._config.authentication_policy,
                self._config.custom_hook_policy,
                self._config.logging_policy,
                policies.DistributedTracingPolicy(**kwargs),
                (
                    policies.SensitiveHeaderCleanupPolicy(**kwargs)
                    if self._config.redirect_policy
                    else None
                ),
                self._config.http_logging_policy,
            ]
        self._client: PipelineClient = PipelineClient(
            base_url=endpoint, policies=_policies, **kwargs
        )

        client_models = {
            k: v for k, v in _models.__dict__.items() if isinstance(v, type)
        }
        self._serialize = Serializer(client_models)
        self._deserialize = Deserializer(client_models)
        self._serialize.client_side_validation = False
        self.well_known = WellKnownOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.auth = AuthOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.config = ConfigOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.jobs = JobsOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.lollygag = LollygagOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.pilots = PilotsOperations(
            self._client, self._config, self._serialize, self._deserialize
        )

    def send_request(
        self, request: HttpRequest, *, stream: bool = False, **kwargs: Any
    ) -> HttpResponse:
        """Runs the network request through the client's chained policies.

        >>> from azure.core.rest import HttpRequest
        >>> request = HttpRequest("GET", "https://www.example.org/")
        <HttpRequest [GET], url: 'https://www.example.org/'>
        >>> response = client.send_request(request)
        <HttpResponse: 200 OK>

        For more information on this code flow, see https://aka.ms/azsdk/dpcodegen/python/send_request

        :param request: The network request you want to make. Required.
        :type request: ~azure.core.rest.HttpRequest
        :keyword bool stream: Whether the response payload will be streamed. Defaults to False.
        :return: The response of your network call. Does not do error handling on your response.
        :rtype: ~azure.core.rest.HttpResponse
        """

        request_copy = deepcopy(request)
        request_copy.url = self._client.format_url(request_copy.url)
        return self._client.send_request(request_copy, stream=stream, **kwargs)  # type: ignore

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Self:
        self._client.__enter__()
        return self

    def __exit__(self, *exc_details: Any) -> None:
        self._client.__exit__(*exc_details)
