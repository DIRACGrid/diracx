"""Tests for the OpenTelemetry setup shared by all the processes."""

from __future__ import annotations

import logging

import pytest
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)

from diracx.core.otel import configure_otel
from diracx.core.otel._setup import _create_exporters, _setup_log_handler
from diracx.core.settings import OTELSettings
from diracx.testing.otel import install_otel_providers


@pytest.fixture(scope="session")
def otel_providers():
    return install_otel_providers()


def test_configure_otel_disabled():
    assert configure_otel("test", settings=OTELSettings(enabled=False)) is None


@pytest.mark.parametrize(
    "settings, module, endpoints",
    [
        (
            OTELSettings(protocol="grpc", grpc_endpoint="collector:4317"),
            "grpc",
            ["collector:4317"] * 3,
        ),
        (
            OTELSettings(protocol="http", http_endpoint="https://collector:4318/"),
            "http",
            [
                "https://collector:4318/v1/traces",
                "https://collector:4318/v1/metrics",
                "https://collector:4318/v1/logs",
            ],
        ),
    ],
)
def test_exporters_follow_the_protocol(settings, module, endpoints):
    exporters = _create_exporters(settings)
    assert [type(e).__module__.split(".")[-2] for e in exporters] == [module] * 3
    assert [e._endpoint for e in exporters] == endpoints


def test_unknown_protocol_is_rejected():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        OTELSettings(protocol="udp")


def test_logs_are_exported_once_with_the_message_as_body(otel_providers):
    tracer_provider, _ = otel_providers
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider()
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    handler = _setup_log_handler(logger_provider, ["diracx.test_otel_logs"])
    logger = logging.getLogger("diracx.test_otel_logs.child")
    logger.setLevel(logging.INFO)
    try:
        with tracer_provider.get_tracer("test").start_as_current_span("x") as span:
            logger.info("hello %s", "world")
            try:
                raise ValueError("boom")
            except ValueError:
                logger.exception("failed")
        # Not below the configured loggers: not exported
        logging.getLogger("sqlalchemy.test_otel_logs").warning("third party")
    finally:
        logging.getLogger("diracx.test_otel_logs").removeHandler(handler)

    records = [r.log_record for r in exporter.get_finished_logs()]
    assert [r.body for r in records] == ["hello world", "failed"]
    assert {r.trace_id for r in records} == {span.get_span_context().trace_id}
    assert records[1].attributes["exception.type"] == "ValueError"
    assert "boom" in records[1].attributes["exception.stacktrace"]


def test_uvicorn_access_logs_are_structured_in_the_exported_records():
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider()
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    access = logging.getLogger("uvicorn.access")
    level = access.level
    access.setLevel(logging.INFO)
    handler = _setup_log_handler(logger_provider, ["uvicorn.access"])
    try:
        access.info(
            '%s - "%s %s HTTP/%s" %d',
            "127.0.0.1:1234",
            "POST",
            "/api/jobs/",
            "1.1",
            201,
        )
    finally:
        access.removeHandler(handler)
        access.setLevel(level)

    [record] = [r.log_record for r in exporter.get_finished_logs()]
    assert record.attributes["http.request.method"] == "POST"
    assert record.attributes["url.path"] == "/api/jobs/"
    assert record.attributes["http.response.status_code"] == 201
    assert record.attributes["client.address"] == "127.0.0.1:1234"
    assert record.attributes["network.protocol.version"] == "1.1"
