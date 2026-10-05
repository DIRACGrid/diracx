"""Tests for the OpenTelemetry instrumentation of the SQL queries."""

from __future__ import annotations

import asyncio

import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from sqlalchemy import text
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from diracx.db.sql import instrument_sqlalchemy
from diracx.testing.otel import SPAN_EXPORTER, install_otel_providers, metric_value


@pytest.fixture(scope="session")
def otel_providers():
    return install_otel_providers()


@pytest.fixture
def spans(otel_providers):
    SPAN_EXPORTER.clear()
    yield SPAN_EXPORTER
    SPAN_EXPORTER.clear()


async def test_sqlalchemy_instrumentation(otel_providers, spans):
    tracer_provider, meter_provider = otel_providers
    uninstrument = instrument_sqlalchemy(tracer_provider, meter_provider)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    error_attrs = {"db.namespace": ":memory:", "error.type": "OperationalError"}
    errors_before = metric_value("db.client.operation.duration", **error_attrs)
    try:
        async with engine.connect() as conn:
            # Outside of any span: metric only, no orphan trace
            await conn.execute(text("SELECT 1"))
            assert spans.get_finished_spans() == ()

            with trace.get_tracer("test").start_as_current_span("request") as parent:
                await conn.execute(text("SELECT 2"))
                with pytest.raises(Exception, match="no such table"):
                    await conn.execute(text("SELECT * FROM missing_table"))
    finally:
        uninstrument()
        await engine.dispose()

    queries = [s for s in spans.get_finished_spans() if s.name == "SELECT :memory:"]
    assert len(queries) == 2
    ok, failed = queries
    for span in queries:
        assert span.kind == SpanKind.CLIENT
        assert span.parent.span_id == parent.get_span_context().span_id
        assert span.attributes["db.system.name"] == "sqlite"
        assert span.attributes["db.operation.name"] == "SELECT"
    assert ok.attributes["db.query.text"] == "SELECT 2"
    assert failed.status.status_code == StatusCode.ERROR
    assert failed.attributes["error.type"] == "OperationalError"
    assert (
        metric_value("db.client.operation.duration", **error_attrs) == errors_before + 1
    )


async def test_sqlalchemy_connection_wait_and_timeouts(otel_providers, tmp_path):
    tracer_provider, meter_provider = otel_providers
    uninstrument = instrument_sqlalchemy(tracer_provider, meter_provider)
    db_path = tmp_path / "pool.db"
    # A single connection, and a short timeout to wait for it
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        poolclass=AsyncAdaptedQueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.2,
    )
    attrs = {"db.client.connection.pool.name": str(db_path)}

    async def hold(seconds: float) -> None:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            await asyncio.sleep(seconds)

    try:
        results = await asyncio.gather(hold(0.5), hold(0), return_exceptions=True)
    finally:
        uninstrument()
        await engine.dispose()

    # The second connection waited for the first one until the timeout
    assert results[0] is None
    assert isinstance(results[1], PoolTimeoutError)
    assert metric_value("db.client.connection.wait_time", **attrs) == 2
    assert metric_value("db.client.connection.timeouts", **attrs) == 1


def test_instrumentation_is_installed_once(otel_providers):
    tracer_provider, meter_provider = otel_providers
    uninstrument = instrument_sqlalchemy(tracer_provider, meter_provider)
    try:
        assert instrument_sqlalchemy(tracer_provider, meter_provider) is uninstrument
    finally:
        uninstrument()
    # Once removed, it can be installed again
    instrument_sqlalchemy(tracer_provider, meter_provider)()
