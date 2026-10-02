# Use the Grafana dashboards

The DiracX chart ships [Grafana](https://grafana.com/) dashboards built on the [DiracX metrics](../../reference/opentelemetry.md#metrics).
This guide explains how to install them and what to look at.

## Prerequisites

- DiracX sends its telemetry to a collector ([Enable OpenTelemetry](enable-opentelemetry.md)).
- The metrics end up in Prometheus, **with the resource attributes as labels** (see [Configure the collector for the dashboards](enable-opentelemetry.md#configure-the-collector-for-the-dashboards)). Without it, the variables of the dashboards stay empty.
- A Grafana with this Prometheus as a data source.

## Install the dashboards

=== "With the chart"

    When Grafana is deployed by the chart (`grafana.enabled: true`, as in the demo), the dashboards of [`diracx/dashboards`](https://github.com/DIRACGrid/diracx-charts/tree/master/diracx/dashboards) are provisioned automatically, in the default folder.

=== "In an existing Grafana"

    1. Download the JSON files from [`diracx/dashboards`](https://github.com/DIRACGrid/diracx-charts/tree/master/diracx/dashboards).
    2. In Grafana, go to *Dashboards > New > Import*, and upload the file.
    3. Repeat for each dashboard.

    The dashboards ask for the Prometheus data source with their *Data source* variable, so there is nothing to change when importing them.

## Choose what to look at

Both dashboards have variables at the top:

| Variable          | Description                                                            |
| ----------------- | ---------------------------------------------------------------------- |
| Data source       | The Prometheus data source                                             |
| Service           | The installation (`service_name`, i.e. `DIRACX_OTEL_APPLICATION_NAME`) |
| Instance / Worker | Restrict to some API server pods, or some worker pods                  |
| Route             | *(DiracX Routers)* Restrict to some routes                             |
| Task              | *(DiracX Tasks)* Restrict to some tasks                                |

The panels have a description (the `i` icon next to their title) explaining what they show.

## DiracX Routers

| Row       | Use it to...                                                                                                                                   |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Overview  | See at a glance the request rate, the share of server errors, the 95th percentile latency and the number of instances                          |
| Traffic   | Find which routes are the busiest, and which ones fail (5xx) or are refused (4xx)                                                              |
| Latency   | Find the slow routes. The table at the bottom summarises every route over the selected time range: sort it by requests, latency or error ratio |
| Instances | Check that the load is spread over the instances, and that no instance accumulates requests in flight                                          |
| Clients   | See which versions of the DiracX client are used, and which ones are refused. The table tells whether the minimum client version can be raised |
| Databases | Find slow or failing SQL queries, connection pools close to exhaustion, and the time spent waiting for a connection                            |

## DiracX Tasks

| Row         | Use it to...                                                                                                                                  |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Overview    | See at a glance the throughput, the failure ratio, the backlog, the delayed tasks, the worker utilisation and whether a scheduler is running  |
| Throughput  | Compare the submission rate with the execution rate: a sustained gap means the workers do not keep up                                         |
| Latency     | Find slow tasks, and check how long tasks wait before being executed, per priority                                                            |
| Queues      | See which streams accumulate a backlog, and detect crashed workers (reclaimed messages)                                                       |
| Workers     | See how busy the workers of each size are, to decide whether to add workers                                                                   |
| Reliability | Follow retries (errors and lock contention), tasks given up, the content of the dead letter queue, rejected messages and periodic submissions |
| Databases   | As for the routers, for the SQL queries of the workers                                                                                        |

See [Troubleshoot with telemetry](troubleshoot-with-telemetry.md) for what to do with what you see.

## Go from a metric to the traces

The metrics tell *that* something is wrong, the traces tell *why*.
Once a dashboard shows, for example, that a route is slow, search the traces in Jaeger or Tempo for that span name (e.g. `POST /api/jobs/search`) with a minimum duration, and look at which child spans (SQL queries, submitted tasks) take the time.
