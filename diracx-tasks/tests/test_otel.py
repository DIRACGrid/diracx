"""Tests for the OpenTelemetry instrumentation of the task system."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import msgpack
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from diracx.core.settings import OTELSettings
from diracx.tasks.otel import _instrument_sqlalchemy, configure_otel
from diracx.tasks.plumbing.broker.models import (
    ReceivedMessage,
    TaskMessage,
    submit_task,
)
from diracx.tasks.plumbing.scheduler.scheduler import collect_stream_stats
from diracx.tasks.plumbing.worker.worker import Worker
from diracx.testing.otel import SPAN_EXPORTER, install_otel_providers, metric_value

from .conftest import get_enqueued_messages


@pytest.fixture(scope="session")
def otel_providers():
    return install_otel_providers()


@pytest.fixture
def spans(otel_providers):
    SPAN_EXPORTER.clear()
    yield SPAN_EXPORTER
    SPAN_EXPORTER.clear()


def span_named(exporter: InMemorySpanExporter, name: str):
    matching = [s for s in exporter.get_finished_spans() if s.name == name]
    assert len(matching) == 1, [s.name for s in exporter.get_finished_spans()]
    return matching[0]


def mock_redis(lock_acquired: bool = True) -> AsyncMock:
    redis = AsyncMock()
    redis.__aenter__ = AsyncMock(return_value=redis)
    redis.__aexit__ = AsyncMock(return_value=False)
    redis.set = AsyncMock(return_value=True if lock_acquired else None)
    redis.zadd = AsyncMock()
    return redis


def make_worker(broker, task_class_registry, wrapped_registry) -> Worker:
    return Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
    )


def test_configure_otel_disabled():
    assert configure_otel("test", settings=OTELSettings(enabled=False)) is None


async def test_task_execution_continues_the_submitter_trace(
    broker, task_class_registry, wrapped_registry, spans
):
    """Request -> task.submit -> task.process -> task.execute must be one trace."""
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("request") as request_span:
        task_id = await submit_task(
            broker,
            "test:SuccessTask",
            labels={"priority": "normal", "size": "small"},
        )

    [message] = await get_enqueued_messages(broker)
    assert "traceparent" in message.trace_context

    worker = make_worker(broker, task_class_registry, wrapped_registry)
    with patch.object(worker, "_get_redis", return_value=mock_redis()):
        await worker.process_message(message.dumpb())

    submit = span_named(spans, "task.submit test:SuccessTask")
    process = span_named(spans, "task.process test:SuccessTask")
    execute = span_named(spans, "task.execute test:SuccessTask")

    assert submit.kind == SpanKind.PRODUCER
    assert process.kind == SpanKind.CONSUMER
    assert submit.parent.span_id == request_span.get_span_context().span_id
    assert process.parent.span_id == submit.context.span_id
    assert execute.parent.span_id == process.context.span_id
    assert {s.context.trace_id for s in (submit, process, execute)} == {
        request_span.get_span_context().trace_id
    }
    assert process.attributes["task.id"] == task_id
    assert execute.attributes["task.status"] == "ok"


async def test_submit_counts_tasks(broker, spans):
    attrs = {"task_name": "test:SuccessTask", "priority": "normal", "delayed": False}
    before = metric_value("tasks_submitted_total", **attrs)
    await submit_task(
        broker, "test:SuccessTask", labels={"priority": "normal", "size": "small"}
    )
    assert metric_value("tasks_submitted_total", **attrs) == before + 1


async def test_failed_task_marks_the_span_as_error(
    broker, task_class_registry, wrapped_registry, spans
):
    worker = make_worker(broker, task_class_registry, wrapped_registry)
    task_msg = TaskMessage(
        task_id="t-fail",
        task_name="test:DLQTask",
        labels={"priority": "normal", "size": "medium"},
        task_args=[],
        task_kwargs={},
    )
    before = metric_value("tasks_failed_total", task_name="test:DLQTask")

    with patch.object(worker, "_get_redis", return_value=mock_redis()):
        result = await worker.run_task(wrapped_registry["test:DLQTask"], task_msg)

    assert result.is_err
    execute = span_named(spans, "task.execute test:DLQTask")
    assert execute.status.status_code == StatusCode.ERROR
    assert execute.attributes["error.type"] == "RuntimeError"
    # The traceback is recorded on the span
    assert [event.name for event in execute.events] == ["exception"]
    assert metric_value("tasks_failed_total", task_name="test:DLQTask") == before + 1


async def test_lock_contention_is_a_retry_not_a_completion(
    broker, task_class_registry, wrapped_registry, spans
):
    worker = make_worker(broker, task_class_registry, wrapped_registry)
    task_msg = TaskMessage(
        task_id="t-locked",
        task_name="test:LockedTask",
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
    )
    completed_before = metric_value(
        "tasks_completed_total", task_name="test:LockedTask"
    )
    retried_before = metric_value(
        "tasks_retried_total", task_name="test:LockedTask", reason="lock_contention"
    )

    with patch.object(
        worker, "_get_redis", return_value=mock_redis(lock_acquired=False)
    ):
        await worker.run_task(wrapped_registry["test:LockedTask"], task_msg)

    assert (
        metric_value("tasks_completed_total", task_name="test:LockedTask")
        == completed_before
    )
    assert (
        metric_value(
            "tasks_retried_total", task_name="test:LockedTask", reason="lock_contention"
        )
        == retried_before + 1
    )
    execute = span_named(spans, "task.execute test:LockedTask")
    assert execute.attributes["task.status"] == "lock_contention"
    assert "task.retry_scheduled" in [event.name for event in execute.events]


async def test_retry_stays_in_the_original_trace(
    broker, task_class_registry, wrapped_registry
):
    worker = make_worker(broker, task_class_registry, wrapped_registry)
    trace_context = {
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    }
    task_msg = TaskMessage(
        task_id="t-retry",
        task_name="test:FailOnceTask",
        labels={"priority": "normal", "size": "small"},
        task_args=[],
        task_kwargs={},
        trace_context=trace_context,
    )
    redis = mock_redis()
    with patch.object(worker, "_get_redis", return_value=redis):
        await worker._schedule_retry(task_msg, datetime.now(tz=UTC), 1, reason="error")

    [(_, mapping), _] = redis.zadd.call_args
    [raw_message] = mapping
    retry_message = TaskMessage.loadb(raw_message)
    assert retry_message.task_id != task_msg.task_id
    assert retry_message.trace_context == trace_context


def test_message_without_trace_context_is_still_valid():
    """Messages produced before the trace context was added must be readable."""
    raw = msgpack.packb(
        {
            "task_id": "old",
            "task_name": "test:SuccessTask",
            "labels": {},
            "task_args": [],
            "task_kwargs": {},
        }
    )
    assert TaskMessage.loadb(raw).trace_context == {}


def test_received_message_enqueued_at():
    async def noop() -> None:
        pass

    message = ReceivedMessage(
        data=b"", ack=noop, renew=noop, message_id="1700000000123-4"
    )
    assert message.enqueued_at == 1700000000.123
    assert ReceivedMessage(data=b"", ack=noop, renew=noop).enqueued_at is None


async def test_collect_stream_stats(broker):
    await submit_task(
        broker, "test:SuccessTask", labels={"priority": "normal", "size": "small"}
    )
    async with Redis(connection_pool=broker.connection_pool) as redis:
        stats = await collect_stream_stats(redis, broker.consumer_group_name)

    assert stats["diracx:tasks:normal:small"].length == 1
    assert stats["diracx:tasks:normal:small"].pending == 0
    assert stats["diracx:tasks:realtime:large"].length == 0


async def test_sqlalchemy_instrumentation(otel_providers, spans):
    tracer_provider, meter_provider = otel_providers
    uninstrument = _instrument_sqlalchemy(tracer_provider, meter_provider)
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
    import asyncio

    from sqlalchemy.exc import TimeoutError as PoolTimeoutError
    from sqlalchemy.pool import AsyncAdaptedQueuePool

    tracer_provider, meter_provider = otel_providers
    uninstrument = _instrument_sqlalchemy(tracer_provider, meter_provider)
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


@pytest.fixture
async def task_db():
    from diracx.tasks.plumbing.persistence.dlq import TaskDB

    db = TaskDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        yield db


async def test_task_db_counts_the_dead_letter_queue(task_db):
    async with task_db:
        await task_db.insert_dlq_task("test:A", b"", 3, last_error="ValueError: boom")
        await task_db.insert_dlq_task("test:A", b"", 3)
        failed = await task_db.insert_dlq_task("test:B", b"", 3)
        await task_db.mark_failed(failed, "still failing")

    async with task_db:
        assert await task_db.count_dlq_tasks() == {
            ("test:A", "PENDING"): 2,
            ("test:B", "FAILED"): 1,
        }
        errors = {task["last_error"] for task in await task_db.get_pending_tasks()}
    assert errors == {"ValueError: boom", None}


async def test_scheduler_reports_the_dead_letter_queue(otel_providers, broker, task_db):
    from diracx.tasks.plumbing.scheduler.scheduler import TaskScheduler

    scheduler = TaskScheduler(
        broker=broker, redis_url="redis://unused", task_db=task_db
    )
    async with task_db:
        dlq_id = await task_db.insert_dlq_task("test:Reported", b"", 3)

    await scheduler.collect_dlq_stats()
    attrs = {"task_name": "test:Reported", "status": "PENDING"}
    # Only the scheduler holding the lock reports it
    assert metric_value("dead_letter_queue_tasks", **attrs) == 0
    scheduler.is_leader = True
    assert metric_value("dead_letter_queue_tasks", **attrs) == 1

    # Once the task is removed, 0 is reported
    async with task_db:
        await task_db.delete_dlq_task(dlq_id)
    await scheduler.collect_dlq_stats()
    assert scheduler._dlq_counts[("test:Reported", "PENDING")] == 0


async def test_worker_stores_the_traceback_in_the_dead_letter_queue(
    broker, task_class_registry, wrapped_registry
):
    task_db = AsyncMock()
    task_db.insert_dlq_task = AsyncMock(return_value=1)
    worker = Worker(
        broker=broker,
        task_registry=wrapped_registry,
        task_class_registry=task_class_registry,
        task_db=task_db,
    )
    task_msg = TaskMessage(
        task_id="t-dlq",
        task_name="test:DLQTask",
        labels={"priority": "normal", "size": "medium"},
        task_args=[],
        task_kwargs={},
    )
    with patch.object(worker, "_get_redis", return_value=mock_redis()):
        result = await worker.run_task(wrapped_registry["test:DLQTask"], task_msg)
        await worker._handle_failure(task_msg, result)

    last_error = task_db.insert_dlq_task.call_args.kwargs["last_error"]
    assert "Traceback" in last_error
    assert "RuntimeError: Always fails" in last_error


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
    from diracx.tasks.otel import _create_exporters

    exporters = _create_exporters(settings)
    assert [type(e).__module__.split(".")[-2] for e in exporters] == [module] * 3
    assert [e._endpoint for e in exporters] == endpoints


def test_unknown_protocol_is_rejected():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        OTELSettings(protocol="udp")


def test_logs_are_exported_once_with_the_message_as_body(otel_providers):
    import logging

    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import (
        InMemoryLogRecordExporter,
        SimpleLogRecordProcessor,
    )

    from diracx.tasks.otel import _setup_log_handler

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
    import logging

    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import (
        InMemoryLogRecordExporter,
        SimpleLogRecordProcessor,
    )

    from diracx.tasks.otel import _setup_log_handler

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
