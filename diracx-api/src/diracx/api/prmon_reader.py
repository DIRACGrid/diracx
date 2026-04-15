"""FIFO-based reader for prmon TSV streaming output.

Reads prmon's tab-separated time-series from a named pipe (FIFO) in
real-time, exposing the latest raw sample for heartbeats and an
on-the-fly compressed series for archival.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from diracx.api.prmon_compress import CHANGING_METRICS, STEADY_METRICS, OnlineCompressor

logger = logging.getLogger(__name__)


class PrmonFifoReader:
    """Async reader that consumes prmon TSV output from a FIFO.

    :param fifo_path: Path to the named pipe (created with os.mkfifo before use).
    :param precision: Compression precision (default 0.05).
    """

    def __init__(self, fifo_path: Path, precision: float = 0.05) -> None:
        self._fifo_path = fifo_path
        self._precision = precision
        self.latest_row: dict[str, int] | None = None
        self._compressor: OnlineCompressor | None = None
        self._headers: list[str] | None = None

    @property
    def compressed_series(self) -> list[dict[str, int]]:
        """The on-the-fly compressed time-series."""
        if self._compressor is None:
            return []
        return self._compressor.compressed

    async def run(self) -> None:
        """Read from the FIFO until EOF. Run as an asyncio task."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._blocking_read)

    def _blocking_read(self) -> None:
        """Blocking FIFO read -- runs in a thread executor."""
        try:
            with open(self._fifo_path) as f:
                header_line = f.readline()
                if not header_line:
                    return
                self._headers = header_line.strip().split("\t")
                present_changing = [m for m in CHANGING_METRICS if m in self._headers]
                present_steady = [m for m in STEADY_METRICS if m in self._headers]
                self._compressor = OnlineCompressor(
                    changing_metrics=present_changing,
                    steady_metrics=present_steady,
                    precision=self._precision,
                )
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    values = line.split("\t")
                    row = {h: int(v) for h, v in zip(self._headers, values)}
                    self.latest_row = row
                    self._compressor.add_row(row)
        except OSError:
            logger.warning("FIFO read error", exc_info=True)
        finally:
            if self._compressor is not None:
                self._compressor.flush()

    def write_compressed(self, output_path: Path) -> None:
        """Write the compressed time-series as a TSV file."""
        if not self._headers or not self.compressed_series:
            return
        with open(output_path, "w") as f:
            f.write("\t".join(self._headers) + "\n")
            for row in self.compressed_series:
                values = [str(row.get(h, 0)) for h in self._headers]
                f.write("\t".join(values) + "\n")
