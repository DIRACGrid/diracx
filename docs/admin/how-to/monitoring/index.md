# Monitoring

DiracX reports traces, metrics and logs using [OpenTelemetry](../../explanations/opentelemetry.md).

- [Enable OpenTelemetry](enable-opentelemetry.md): send the telemetry of DiracX to a collector, and configure the collector
- [Use the Grafana dashboards](use-the-dashboards.md): install the dashboards shipped with the chart and read them
- [Troubleshoot with telemetry](troubleshoot-with-telemetry.md): find the cause of common problems (slow requests, failing or piling up tasks, database pool exhaustion...)
- [Collect and read the logs](collect-and-read-logs.md): write the logs as JSON, collect them, and follow them live with stern

The list of spans and metrics is in the [OpenTelemetry reference](../../reference/opentelemetry.md), and the log formats in the [logs reference](../../reference/logs.md).
