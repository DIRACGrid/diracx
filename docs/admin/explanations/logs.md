# Logs

This page explains how DiracX writes its logs, and why.
The formats and fields are listed in the [logs reference](../reference/logs.md), and [Collect and read the logs](../how-to/monitoring/collect-and-read-logs.md) explains how to use them.

## One configuration for all the processes

The API servers, the scheduler, the workers and the `diracx-tasks` command line all configure their logs in the same way, from the `DIRACX_LOG_*` settings:

- the logs are written to stderr, where container platforms collect them;
- the loggers of DiracX and of its extension log at `DIRACX_LOG_LEVEL` (`INFO`), while the other libraries only report their warnings (`DIRACX_LOG_LIBRARIES_LEVEL`): their `INFO` messages (every HTTP request made by httpx, every command started by `sh`...) would drown the DiracX ones;
- the access logs of uvicorn use the same format as the rest.

## Text or JSON

The text format is meant for humans: a terminal, `kubectl logs`, `pixi run local-start`.

The JSON format is meant for machines. Log collectors (fluent-bit, filebeat, promtail, the OpenTelemetry collector...) read the output of the containers line by line. With text logs, they have to guess the structure of each line with regular expressions, and a traceback, written on several lines, becomes several unrelated records.
With one JSON object per line, each log is exactly one record, its fields (level, logger, task, user...) are fields in the log backend, and the traceback is one of them.

The field names are those of the [OpenTelemetry log data model](https://opentelemetry.io/docs/specs/otel/logs/data-model/): the records look the same whether they are collected from stderr or sent over OpenTelemetry, and whatever collects them.

Reading JSON logs directly is tedious, which is why the text format remains the default, and why tools such as [stern](https://github.com/stern/stern) with the DiracX template format them for a terminal.

## Context

A log line such as `Task failed after 3 attempts` is only useful if one knows *which* task, or *whose* request.
Rather than repeating this information in every message, DiracX adds it to all the records emitted in a given context:

- while a worker processes a task: the name and ID of the task;
- during a request, once the token is validated: the user, the VO and the group.

It becomes possible to get all the logs of a task, or of a user, with a simple filter in the log backend.
The context is kept in a Python `ContextVar`, so that concurrent requests and tasks, which run in the same process, each have their own.

When OpenTelemetry is enabled, the JSON logs also carry the trace and span IDs, so that one can go from a log to the trace of the request or task, and back.

## Two ways of collecting the logs

The logs can be collected from stderr (JSON), or sent over OpenTelemetry (OTLP) when it is enabled. Both carry the same information.

- Collecting stderr also gets the logs of the other libraries, the lines written before the logging is configured, and the crashes of the process. It requires a log agent on the nodes, which most clusters have.
- OTLP needs nothing else than the OpenTelemetry collector, and the records need no parsing, but only the records of the DiracX loggers are sent.

Using both stores every log twice: choose one.
