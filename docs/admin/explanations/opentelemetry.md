# OpenTelemetry

> :warning: **Experimental**: opentelemetry is an evolving product, and so is our implementation of it.

`diracx` is capable of sending [OpenTelemetry](https://opentelemetry.io/) data to a collector. The settings are controlled by the
`diracx.routers.otel.OTELSettings` classes
`diracx` will then export metrics, traces, and logs. For the moment, nothing is really instrumented, but the infrastructure is there

![OTEL Logs](https://diracx-docs-static.s3.cern.ch/assets/images/admin/explanations/otel/otel-logs.png)
![OTEL Metrics](https://diracx-docs-static.s3.cern.ch/assets/images/admin/explanations/otel/otel-metrics.png)
![OTEL Traces](https://diracx-docs-static.s3.cern.ch/assets/images/admin/explanations/otel/otel-traces.png)
