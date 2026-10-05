"""Entry point to configure OpenTelemetry, importable without the SDK."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from diracx.core.settings import OTELSettings

if TYPE_CHECKING:
    from ._setup import OTELProviders


def configure_otel(
    component: str,
    *,
    extra_logger_names: Iterable[str] = (),
    settings: OTELSettings | None = None,
) -> OTELProviders | None:
    """Configure the process to send OpenTelemetry data.

    Metrics, Traces and Logs are sent to an OTEL collector.
    The Collector can then redirect it to whatever is configured.
    Typically: Jaeger for traces, Prometheus for metrics, ElasticSearch for logs.

    The providers are registered globally, so code instrumented with the
    ``opentelemetry-api`` (e.g. ``trace.get_tracer(__name__)``) automatically
    exports through them, even if the tracer/meter was created at import time.

    Args:
        component: Name of the DiracX component (e.g. ``routers``, ``tasks-worker``),
            exported as the ``diracx.component`` resource attribute.
        extra_logger_names: Loggers, in addition to those of DiracX and its
            extension, to which the OTEL log handler is attached. Needed for
            loggers which do not propagate (e.g. uvicorn's).
        settings: The settings to use. Read from the environment if not given.

    Returns:
        The installed providers, or ``None`` if OpenTelemetry is disabled.

    """
    if settings is None:
        settings = OTELSettings()
    if not settings.enabled:
        return None

    try:
        from ._setup import configure_providers
    except ImportError as exc:
        raise ImportError(
            "OpenTelemetry is enabled (DIRACX_OTEL_ENABLED) but its SDK is not "
            "installed: install diracx-core[otel]"
        ) from exc

    return configure_providers(
        component, extra_logger_names=extra_logger_names, settings=settings
    )
