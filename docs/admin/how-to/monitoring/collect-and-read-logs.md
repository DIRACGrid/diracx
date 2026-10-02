# Collect and read the logs

This guide explains how to choose the format of the DiracX logs, how to collect them, and how to read them live.
See the [logs reference](../../reference/logs.md) for the list of fields, and the [logs explanation](../../explanations/logs.md) for the reasoning.

## Choose the format

All the DiracX processes write their logs to stderr, in the format given by `DIRACX_LOG_FORMAT`:

- `text` (the default): human readable, for a terminal;
- `json`: one JSON object per line, for log collectors. This is what the DiracX chart uses.

With the helm chart, the format and the levels are set in `diracx.settings`, which is shared by all the processes:

```yaml
diracx:
  settings:
    DIRACX_LOG_FORMAT: "json"
    # Level of the DiracX (and extension) loggers
    DIRACX_LOG_LEVEL: "INFO"
    # Level of the other libraries (SQLAlchemy, httpx...)
    DIRACX_LOG_LIBRARIES_LEVEL: "WARNING"
```

## Collect the logs

There are two ways to get the logs into a log backend (Elasticsearch, OpenSearch, Loki...).
**Use only one of them**, otherwise every log is stored twice.

=== "From stderr (JSON)"

    The log agent of the cluster (fluent-bit, filebeat, promtail, or the OpenTelemetry collector with its `filelog` receiver) reads the output of the containers.
    With `DIRACX_LOG_FORMAT=json`, it only has to parse each line as JSON: there is no regular expression to maintain, and a traceback stays in its record.

    For example, with the OpenTelemetry collector (`opentelemetry-collector.presets.logsCollection.enabled: true` in the chart reads the container logs), add these operators to the `filelog` receiver:

    ```yaml
    receivers:
      filelog:
        operators:
          # ... the operators parsing the container runtime format ...
          # DiracX writes one JSON object per line
          - type: json_parser
            if: 'body matches "^\\{"'
            parse_to: attributes
            timestamp:
              parse_from: attributes.timestamp
              layout_type: gotime
              layout: "2006-01-02T15:04:05.999Z07:00"
            severity:
              parse_from: attributes.severity_text
          # Link the record to its trace
          - type: trace_parser
            if: 'attributes.trace_id != nil'
            trace_id:
              parse_from: attributes.trace_id
            span_id:
              parse_from: attributes.span_id
          - type: move
            if: 'attributes.body != nil'
            from: attributes.body
            to: body
          - type: remove
            if: 'attributes.timestamp != nil'
            field: attributes.timestamp
    ```

    The lines which are not JSON (e.g. written before the logging is configured) are kept as they are.

=== "With OpenTelemetry (OTLP)"

    When [OpenTelemetry is enabled](enable-opentelemetry.md), DiracX also sends its log records to the collector, with the same fields.
    Nothing has to be parsed, but only the records of the DiracX loggers are sent: not those of the other libraries, nor anything written directly to stderr.

## Read the logs live

### With stern

[stern](https://github.com/stern/stern) follows the logs of several pods at once, and can format JSON logs with a template.
DiracX provides one: [download `diracx.stern.tmpl`](diracx.stern.tmpl).

```bash
# All the DiracX pods of the namespace
stern --template-file diracx.stern.tmpl diracx
# Only the task workers, and only the errors
stern --template-file diracx.stern.tmpl -l app.kubernetes.io/component=task-worker --include '"severity_text": "ERROR"' diracx
```

Each line shows the pod, the time (UTC), the colored level, the logger and the message, followed by the context (task, user, VO, trace) and the traceback if any:

```text
diracx-demo-task-worker-small-7d9f 21:30:35.173Z INFO diracx.tasks.plumbing.worker.worker Executing task lollygag:SyncOwnersTask (ID: 6bd6549ba1e5) task=lollygag:SyncOwnersTask task_id=6bd6549ba1e5
diracx-demo-task-worker-small-7d9f 21:30:35.173Z ERROR diracx.tasks.plumbing.worker.worker Exception in task lollygag:SyncOwnersTask task=lollygag:SyncOwnersTask task_id=6bd6549ba1e5
Traceback (most recent call last):
  ...
TypeError: SyncOwnersTask.__init__() missing 1 required positional argument: 'owner_name'
diracx-demo-7c9d7d8b4f-xk2lp 21:30:35.173Z INFO diracx.routers.jobs Submitted 3 jobs user=75212b23-14c2 vo=lhcb trace=0af7651916cd43dd8448eb211c80319c
diracx-demo-7c9d7d8b4f-xk2lp 21:30:35.173Z INFO uvicorn.access 10.0.0.7:51234 - "POST /api/jobs/jdl HTTP/1.1" 201 status=201
```

The lines which are not JSON are printed as they are.
To always use the template, put `template-file: /path/to/diracx.stern.tmpl` in `~/.config/stern/config.yaml`.

??? note "The template"

    ```text
    --8<-- "docs/admin/how-to/monitoring/diracx.stern.tmpl"
    ```

### Without stern

- `kubectl logs -f <pod> | jq -r '"\(.timestamp) \(.severity_text) \(.logger): \(.body)"'` formats the logs of a single pod (`jq -R 'fromjson? // .'` also accepts the lines which are not JSON);
- [lnav](https://lnav.org/) and [hl](https://github.com/pamburus/hl) recognise JSON lines and display them in columns, with filters: `kubectl logs <pod> | hl`.

### On a development machine

`pixi run local-start` uses the text format. To see the JSON logs, start it with `DIRACX_LOG_FORMAT=json pixi run local-start`.
