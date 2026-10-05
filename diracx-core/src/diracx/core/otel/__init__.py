"""OpenTelemetry initialization shared by all DiracX processes.

Both the routers (``diracx.routers.otel``) and the long running processes of
the task system (``diracx-tasks worker`` and ``diracx-tasks scheduler``) call
:func:`configure_otel` so that every process exports its traces, metrics and
logs in exactly the same way, driven by
:class:`diracx.core.settings.OTELSettings`.

The OpenTelemetry SDK and exporters are only needed when OpenTelemetry is
enabled, and are installed with the ``otel`` extra (``diracx-core[otel]``).
This module can be imported without them.

Note: this is highly experimental, and OpenTelemetry is a quickly moving target
"""

from __future__ import annotations

__all__ = ["configure_otel"]

from .configure import configure_otel
