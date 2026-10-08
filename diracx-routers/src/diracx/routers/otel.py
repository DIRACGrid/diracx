from __future__ import annotations

__all__ = ["instrument_otel", "record_client_version"]

import logging
import os
import threading

from fastapi import FastAPI
from packaging.version import InvalidVersion, Version

# Required by FastAPIInstrumentor
# to follow semantic conventions for HTTP metrics
# https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http"

from opentelemetry import metrics, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.util.http import parse_excluded_urls

from diracx.core.otel import configure_otel
from diracx.db.sql import instrument_sqlalchemy

# The kubernetes probes are called every few seconds,
# and would drown the meaningful traces and skew the latency metrics
DEFAULT_EXCLUDED_URLS = "api/health/"

_meter = metrics.get_meter(__name__)
_client_requests = _meter.create_counter(
    "client_requests_total",
    description=(
        "Requests per version of the DiracX client (DiracX-Client-Version header), "
        "accepted or rejected because the client is too old"
    ),
)
# The version comes from a header, which anyone can set: bound the number
# of distinct versions reported, the others are reported as "other"
MAX_CLIENT_VERSIONS = 50
_client_versions: set[str] = set()
_client_versions_lock = threading.Lock()
_excluded_urls = parse_excluded_urls(
    os.environ.get("OTEL_PYTHON_FASTAPI_EXCLUDED_URLS", DEFAULT_EXCLUDED_URLS)
)


def _normalise_client_version(header: str | None) -> str:
    if not header:
        # e.g. the web interface, or a direct HTTP request
        return "none"
    try:
        version = str(Version(header))
    except InvalidVersion:
        return "invalid"
    with _client_versions_lock:
        if version in _client_versions:
            return version
        if len(_client_versions) < MAX_CLIENT_VERSIONS:
            _client_versions.add(version)
            return version
    return "other"


def record_client_version(url: str, header: str | None, *, rejected: bool) -> None:
    """Count a request per client version, and tag its span with the version.

    Called by ``ClientMinVersionCheckMiddleware`` for every request.
    """
    if _excluded_urls.url_disabled(url):
        return
    version = _normalise_client_version(header)
    trace.get_current_span().set_attribute(
        "diracx.client.version", (header or "none")[:64]
    )
    _client_requests.add(
        1,
        attributes={
            "client_version": version,
            "outcome": "rejected" if rejected else "accepted",
        },
    )


def instrument_otel(app: FastAPI) -> None:
    """Instrument the application to send OpenTelemetryData.

    The common setup (traces, metrics and logs exporters) is done by
    :func:`diracx.core.otel.configure_otel`, and is controlled by
    :class:`diracx.core.settings.OTELSettings`. The SQL queries are
    instrumented by :func:`diracx.db.sql.instrument_sqlalchemy`.
    On top of that, the FastAPI application itself is instrumented, which gives
    a span per request and the ``http.server.*`` metrics.
    """
    # Add the handler to all uvicorn loggers.
    # Note adding it to just 'uvicorn' or the root logger
    # is not enough because uvicorn sets propagate=False
    uvicorn_loggers = [
        logger_name
        for logger_name in logging.root.manager.loggerDict
        if "uvicorn" in logger_name
    ]
    providers = configure_otel("routers", extra_logger_names=uvicorn_loggers)
    if providers is None:
        return
    instrument_sqlalchemy(providers.tracer_provider, providers.meter_provider)

    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=providers.tracer_provider,
        meter_provider=providers.meter_provider,
        # Can be overridden with OTEL_PYTHON_FASTAPI_EXCLUDED_URLS
        excluded_urls=(
            None
            if "OTEL_PYTHON_FASTAPI_EXCLUDED_URLS" in os.environ
            else DEFAULT_EXCLUDED_URLS
        ),
        # Do not create a span for each ASGI message: a streamed response
        # would otherwise create hundreds of meaningless spans
        exclude_spans=["receive", "send"],
    )
