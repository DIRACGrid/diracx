# Enable OpenTelemetry

This guide explains how to send the telemetry of a DiracX installation to an [OpenTelemetry collector](https://opentelemetry.io/docs/collector/), and how to configure the collector so that the [DiracX dashboards](use-the-dashboards.md) work.

## Point DiracX to a collector

DiracX sends its traces, metrics and logs to a collector using OTLP, over gRPC (the default) or over HTTP.
Set the following [environment variables](../../reference/opentelemetry.md#configuration) for **all** the DiracX processes (API servers, scheduler and workers).
With the helm chart, this is done in `diracx.settings`, which is shared by all of them:

=== "gRPC"

    ```yaml
    diracx:
      settings:
        DIRACX_OTEL_ENABLED: "true"
        # The OTLP/gRPC receiver of your collector
        DIRACX_OTEL_GRPC_ENDPOINT: "my-otel-collector:4317"
        # Set to "false" if the collector uses TLS
        DIRACX_OTEL_GRPC_INSECURE: "true"
        # Identifies the installation: use a different value for each installation
        DIRACX_OTEL_APPLICATION_NAME: "lhcbdiracx-prod"
    ```

=== "HTTP"

    Use HTTP when gRPC cannot go through your network (e.g. an HTTP-only proxy or load balancer in front of the collector):

    ```yaml
    diracx:
      settings:
        DIRACX_OTEL_ENABLED: "true"
        DIRACX_OTEL_PROTOCOL: "http"
        # The base URL of the OTLP/HTTP receiver of your collector:
        # /v1/traces, /v1/metrics and /v1/logs are appended to it.
        # Use https:// if the collector uses TLS
        DIRACX_OTEL_HTTP_ENDPOINT: "http://my-otel-collector:4318"
        # Identifies the installation: use a different value for each installation
        DIRACX_OTEL_APPLICATION_NAME: "lhcbdiracx-prod"
    ```

    The collector must have the `http` protocol enabled in its `otlp` receiver (as in the chart).

If the collector requires authentication or multi-tenancy headers, add them as JSON:

```yaml
    DIRACX_OTEL_HEADERS: '{"tenant_id": "lhcbdiracx-prod"}'
```

The metrics are sent every minute; set `OTEL_METRIC_EXPORT_INTERVAL` (in milliseconds) to change it.

The DiracX container images include the OpenTelemetry SDK. If you install DiracX with `pip`, install `diracx-core[otel]` too: with `DIRACX_OTEL_ENABLED` set and without the SDK, the processes refuse to start.

Once restarted, the processes start sending data; DiracX does not log anything particular about it.
If the collector cannot be reached, the OpenTelemetry exporters log warnings (`Transient error ... exporting ...`), but DiracX itself keeps working normally.

## Deploy a collector with the chart

The chart can deploy a collector, which is what the demo does.
Enable it and point DiracX to it:

```yaml
diracx:
  settings:
    DIRACX_OTEL_ENABLED: "true"
    DIRACX_OTEL_GRPC_ENDPOINT: "<release name>-opentelemetry-collector:4317"

opentelemetry-collector:
  enabled: true
```

The default configuration of the chart exports the metrics for Prometheus (on port `8889`), the traces to Jaeger and the logs to Elasticsearch.
Everything under `opentelemetry-collector.config` can be changed to use other backends: see the [collector configuration documentation](https://opentelemetry.io/docs/collector/configuration/).

## Configure the collector for the dashboards

The [DiracX dashboards](use-the-dashboards.md) identify the processes with the `service_name`, `service_instance_id` and `diracx_component` labels.
These come from resource attributes, which the collector does not turn into labels by default.

=== "Prometheus exporter (scraped)"

    This is what the chart does:

    ```yaml
    exporters:
      prometheus:
        endpoint: ":8889"
        resource_to_telemetry_conversion:
          enabled: true
    ```

=== "Prometheus OTLP receiver (pushed)"

    If the collector pushes to Prometheus (version 3 or later, started with `--web.enable-otlp-receiver`), promote the attributes in the Prometheus configuration:

    ```yaml
    otlp:
      promote_resource_attributes:
        - service.name
        - service.instance.id
        - service.version
        - diracx.component
    ```

## Sample the traces

By default, every trace is kept.
On a busy installation, keep a fraction of them with the standard OpenTelemetry variables:

```yaml
diracx:
  settings:
    # Keep 10% of the traces. "parentbased" means that a trace is either
    # kept or dropped as a whole. The execution of a task is a separate
    # trace, so it is kept or dropped independently of its submission.
    OTEL_TRACES_SAMPLER: "parentbased_traceidratio"
    OTEL_TRACES_SAMPLER_ARG: "0.1"
```

Sampling does not affect the metrics, which are always complete.
To keep all the failed traces while sampling the others, use [tail sampling](https://opentelemetry.io/docs/concepts/sampling/#tail-sampling) in the collector instead.

## Add information to all the telemetry

Additional resource attributes can be set with `OTEL_RESOURCE_ATTRIBUTES`, for example to distinguish the environments sending to the same collector:

```yaml
diracx:
  settings:
    OTEL_RESOURCE_ATTRIBUTES: "deployment.environment.name=production"
```

## Choose which requests are instrumented

The kubernetes probes (`/api/health/...`) are neither traced nor measured.
To change that, set a comma separated list of regular expressions:

```yaml
diracx:
  settings:
    # Also exclude the well-known endpoints
    OTEL_PYTHON_FASTAPI_EXCLUDED_URLS: "api/health/,\\.well-known/"
```

## Check that it works

Look at the telemetry received by the collector, e.g. by adding the [`debug` exporter](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/debugexporter) to its pipelines and looking at its logs.
You should see spans from `diracx.component` `routers` as soon as a request is made, and from `tasks-scheduler` and `tasks-worker` as soon as a periodic task runs.

To try it on a development machine without any infrastructure, see `pixi run local-start --otel` in the [monitor the task system tutorial](../../tutorials/monitor-tasks.md).
