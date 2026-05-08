"""Online (streaming) interpolation-drop compression for prmon time-series data.

Ports the algorithm from HSF/prmon's prmon_compress_output.py for streaming use
without pandas.
"""

from __future__ import annotations

CHANGING_METRICS = [
    "vmem",
    "pss",
    "rss",
    "swap",
    "rchar",
    "wchar",
    "read_bytes",
    "write_bytes",
    "rx_packets",
    "tx_packets",
    "rx_bytes",
    "tx_bytes",
    "gpufbmem",
    "gpumempct",
    "gpusmpct",
    "utime",
    "stime",
]

STEADY_METRICS = [
    "nprocs",
    "nthreads",
    "ngpus",
]


class OnlineCompressor:
    """Streaming interpolation-drop compressor for prmon time-series data.

    Maintains a 3-point sliding window (anchor, pending, new). When a new row
    arrives, checks whether the pending row can be dropped — i.e. all changing
    metrics are within tolerance AND no steady metric changed.

    Attributes:
        compressed: List of committed significant rows (dicts of metric -> value).

    """

    compressed: list[dict[str, int]]

    def __init__(
        self,
        changing_metrics: list[str] | None = None,
        steady_metrics: list[str] | None = None,
        precision: float = 0.05,
    ) -> None:
        self.changing_metrics = (
            changing_metrics if changing_metrics is not None else CHANGING_METRICS
        )
        self.steady_metrics = (
            steady_metrics if steady_metrics is not None else STEADY_METRICS
        )
        self.precision = precision

        self.compressed = []
        self._anchor: dict[str, int] | None = None
        self._pending: dict[str, int] | None = None

        # Track running dynamic range (max - min seen so far) per changing metric
        self._min: dict[str, float] = {}
        self._max: dict[str, float] = {}

    def _update_dynamic_range(self, row: dict[str, int]) -> None:
        """Update running min/max for each changing metric from a new row."""
        for metric in self.changing_metrics:
            if metric not in row:
                continue
            value = row[metric]
            if metric not in self._min:
                self._min[metric] = value
                self._max[metric] = value
            else:
                if value < self._min[metric]:
                    self._min[metric] = value
                if value > self._max[metric]:
                    self._max[metric] = value

    def _can_drop(
        self,
        anchor: dict[str, int],
        middle: dict[str, int],
        new: dict[str, int],
    ) -> bool:
        """Return True if the middle row can be dropped.

        For each changing metric: compute eps = dynamic_range * precision,
        interpolate middle from (anchor, new) using Time as x-axis, and check
        whether the actual middle value is within eps of the interpolation.

        For each steady metric: check that the value did not change between
        anchor and middle.
        """
        # Check steady metrics first: keep the middle row if its value differs
        # from the anchor OR if the new row's value differs from the middle.
        # This preserves both the last row before a change and the first row
        # after the change.
        for metric in self.steady_metrics:
            if metric not in anchor or metric not in middle:
                continue
            if anchor[metric] != middle[metric]:
                return False
            if metric in new and middle[metric] != new[metric]:
                return False

        # Check changing metrics via interpolation
        t_anchor = anchor["Time"]
        t_middle = middle["Time"]
        t_new = new["Time"]

        # Guard: if anchor and new share the same time, cannot interpolate
        if t_new == t_anchor:
            return False

        alpha = (t_middle - t_anchor) / (t_new - t_anchor)

        for metric in self.changing_metrics:
            if metric not in anchor or metric not in middle or metric not in new:
                continue

            dynamic_range = self._max.get(metric, 0) - self._min.get(metric, 0)
            eps = dynamic_range * self.precision

            interpolated = anchor[metric] + alpha * (new[metric] - anchor[metric])
            deviation = abs(middle[metric] - interpolated)

            if deviation > eps:
                return False

        return True

    def add_row(self, row: dict[str, int]) -> None:
        """Process one row of prmon data.

        Maintains the sliding window and commits rows that cannot be dropped.
        """
        self._update_dynamic_range(row)

        if self._anchor is None:
            # First row: commit immediately as anchor
            self.compressed.append(row)
            self._anchor = row
            return

        if self._pending is None:
            # Second row: hold as pending candidate
            self._pending = row
            return

        # Third+ row: evaluate whether pending can be dropped
        if self._can_drop(self._anchor, self._pending, row):
            # Drop pending; new row becomes the pending candidate
            self._pending = row
        else:
            # Pending is significant: commit it, advance anchor, new row is pending
            self.compressed.append(self._pending)
            self._anchor = self._pending
            self._pending = row

    def flush(self) -> None:
        """Commit any remaining buffered (pending) row at EOF."""
        if self._pending is not None:
            self.compressed.append(self._pending)
            self._pending = None
