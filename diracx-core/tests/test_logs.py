"""Tests for the configuration of the logs."""

from __future__ import annotations

import json
import logging

import pytest

from diracx.core import logs
from diracx.core.logs import configure_logging
from diracx.core.settings import LoggingSettings


@pytest.fixture(autouse=True)
def restore_logging(monkeypatch):
    """Restore the global logging state modified by configure_logging."""
    root = logging.getLogger()
    loggers = [
        root,
        *(logging.getLogger(n) for n in ("diracx", "uvicorn", "uvicorn.access")),
    ]
    saved = [(lg, lg.handlers[:], lg.level, lg.propagate) for lg in loggers]
    monkeypatch.setattr(logs, "_handler", None)
    monkeypatch.setattr(logs, "_trace_context_getter", None)
    yield
    for lg, handlers, level, propagate in saved:
        lg.handlers = handlers
        lg.setLevel(level)
        lg.propagate = propagate


def test_configure_logging_is_idempotent():
    first = configure_logging()
    second = configure_logging()
    root_handlers = logging.getLogger().handlers
    assert second in root_handlers
    assert first not in root_handlers


def test_levels(capsys):
    configure_logging(LoggingSettings(level="DEBUG", libraries_level="ERROR"))

    logging.getLogger("diracx.test_logs").debug("diracx debug")
    logging.getLogger("some.library").warning("library warning")
    logging.getLogger("some.library").error("library error")

    err = capsys.readouterr().err
    assert "DEBUG    diracx.test_logs: diracx debug" in err
    assert "library warning" not in err
    assert "ERROR    some.library: library error" in err


def test_uvicorn_console_handlers_are_replaced():
    class OtherHandler(logging.Handler):
        def emit(self, record):
            pass

    access = logging.getLogger("uvicorn.access")
    # As configured by uvicorn
    access.propagate = False
    uvicorn_console = logging.StreamHandler()
    other = OtherHandler()
    access.handlers = [uvicorn_console, other]

    handler = configure_logging()

    assert access.handlers == [other, handler]


def json_lines(err: str) -> list[dict]:
    return [json.loads(line) for line in err.splitlines() if line.startswith("{")]


def test_json_format(capsys):
    configure_logging(LoggingSettings(format="json"))
    logger = logging.getLogger("diracx.test_logs")

    logger.info("job %d submitted", 42, extra={"job_id": 42, "obj": object()})
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("failed")

    submitted, failed = json_lines(capsys.readouterr().err)
    assert submitted["severity_text"] == "INFO"
    assert submitted["logger"] == "diracx.test_logs"
    assert submitted["body"] == "job 42 submitted"
    assert submitted["timestamp"].endswith("Z")
    # Extra attributes are fields, and are always serialisable
    assert submitted["job_id"] == 42
    assert submitted["obj"].startswith("<object object")
    assert "trace_id" not in submitted
    # The traceback is in the record, not on separate lines
    assert failed["exception.type"] == "ValueError"
    assert failed["exception.message"] == "boom"
    assert "Traceback" in failed["exception.stacktrace"]


def test_json_trace_context(capsys):
    configure_logging(LoggingSettings(format="json"))
    logs.set_trace_context_getter(lambda: ("a" * 32, "b" * 16))

    logging.getLogger("diracx.test_logs").warning("in a span")

    [entry] = json_lines(capsys.readouterr().err)
    assert entry["trace_id"] == "a" * 32
    assert entry["span_id"] == "b" * 16


def test_json_uvicorn_access_logs(capsys):
    access = logging.getLogger("uvicorn.access")
    access.propagate = False
    access.setLevel(logging.INFO)
    configure_logging(LoggingSettings(format="json"))

    access.info(
        '%s - "%s %s HTTP/%s" %d', "127.0.0.1:1234", "GET", "/api/jobs", "1.1", 200
    )

    [entry] = json_lines(capsys.readouterr().err)
    assert entry["http.request.method"] == "GET"
    assert entry["url.path"] == "/api/jobs"
    assert entry["http.response.status_code"] == 200
    assert entry["client.address"] == "127.0.0.1:1234"


def test_log_context_in_both_formats(capsys):
    from diracx.core.logs import log_context

    logger = logging.getLogger("diracx.test_logs")
    for log_format in ("text", "json"):
        configure_logging(LoggingSettings(format=log_format))
        with log_context(**{"task.name": "jobs:X", "task.id": "abc"}):
            logger.info("in the task")
            # Explicit attributes take precedence (and do not raise)
            logger.info("explicit", extra={"task.id": "explicit"})
        logger.info("outside")

    err = capsys.readouterr().err
    text, json_part = err.split("\n{", 1)
    assert "in the task [task.name=jobs:X task.id=abc]" in text
    assert "explicit [task.name=jobs:X task.id=explicit]" in text
    assert "diracx.test_logs: outside" in text
    assert "outside [" not in text
    in_task, explicit, outside = json_lines("{" + json_part)
    assert (in_task["task.name"], in_task["task.id"]) == ("jobs:X", "abc")
    assert explicit["task.id"] == "explicit"
    assert "task.name" not in outside


async def test_set_log_context_is_isolated_per_task(capsys):
    import asyncio

    from diracx.core.logs import set_log_context

    configure_logging(LoggingSettings(format="json"))
    logger = logging.getLogger("diracx.test_logs")

    async def request(user: str) -> None:
        set_log_context(**{"enduser.id": user})
        await asyncio.sleep(0)
        logger.info("request of %s", user)

    # Each request runs in its own task, as in uvicorn
    await asyncio.gather(request("alice"), request("bob"))
    logger.info("after")

    entries = json_lines(capsys.readouterr().err)
    assert {(e["body"], e.get("enduser.id")) for e in entries} == {
        ("request of alice", "alice"),
        ("request of bob", "bob"),
        ("after", None),
    }


def test_text_format_is_in_utc(capsys, monkeypatch):
    import time

    # A timezone far from UTC: the date must not depend on it
    monkeypatch.setenv("TZ", "Pacific/Kiritimati")
    time.tzset()
    try:
        configure_logging()
        record = logging.LogRecord(
            "diracx.test_logs", logging.INFO, "", 0, "x", None, None
        )
        record.created = 0.123
        logging.getLogger("diracx.test_logs").handle(record)
    finally:
        monkeypatch.undo()
        time.tzset()

    assert capsys.readouterr().err.startswith(
        "1970-01-01T00:00:00.123Z INFO     diracx.test_logs: x"
    )
