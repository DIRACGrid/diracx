"""Tests for PrmonFifoReader."""

from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path

import pytest

SAMPLE_HEADER = "Time\twtime\tpss\trss\tswap\tvmem\tutime\tstime\tnprocs\tnthreads"
SAMPLE_ROW_1 = "1713000001\t1\t15000\t20000\t0\t50000\t1\t0\t1\t4"
SAMPLE_ROW_2 = "1713000002\t2\t18000\t24000\t0\t55000\t2\t1\t1\t4"
SAMPLE_ROW_3 = "1713000003\t3\t18100\t24100\t0\t55100\t3\t1\t1\t4"


def _write_to_fifo(fifo_path: Path, lines: list[str], delay: float = 0.05) -> None:
    """Write lines to a FIFO from a separate thread (simulates prmon)."""
    import time

    with open(fifo_path, "w") as f:
        for line in lines:
            f.write(line + "\n")
            f.flush()
            time.sleep(delay)


@pytest.mark.asyncio
async def test_fifo_reader_latest_row(tmp_path: Path):
    """PrmonFifoReader.latest_row should reflect the most recent sample."""
    from diracx.api.prmon_reader import PrmonFifoReader

    fifo_path = tmp_path / "prmon_fifo"
    os.mkfifo(fifo_path)

    reader = PrmonFifoReader(fifo_path)
    reader_task = asyncio.create_task(reader.run())
    # Yield so the reader task starts and blocks on FIFO open in executor
    await asyncio.sleep(0)

    # Write 3 rows from a thread
    writer = threading.Thread(
        target=_write_to_fifo,
        args=(fifo_path, [SAMPLE_HEADER, SAMPLE_ROW_1, SAMPLE_ROW_2, SAMPLE_ROW_3]),
    )
    writer.start()

    # Wait for reader to consume all rows (non-blocking to avoid deadlock)
    await asyncio.to_thread(writer.join)
    await asyncio.sleep(0.2)

    assert reader.latest_row is not None
    assert reader.latest_row["wtime"] == 3
    assert reader.latest_row["pss"] == 18100

    reader_task.cancel()
    try:
        await reader_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_fifo_reader_compressed_series(tmp_path: Path):
    """Compressed series should contain fewer rows than raw input."""
    from diracx.api.prmon_reader import PrmonFifoReader

    fifo_path = tmp_path / "prmon_fifo"
    os.mkfifo(fifo_path)

    reader = PrmonFifoReader(fifo_path)
    reader_task = asyncio.create_task(reader.run())
    await asyncio.sleep(0)

    # Write 10 rows of perfectly linear pss growth (highly compressible)
    lines = [SAMPLE_HEADER]
    for t in range(10):
        lines.append(
            f"{1713000000 + t}\t{t}\t{1000 * (t + 1)}\t{2000 * (t + 1)}\t0"
            f"\t{5000 * (t + 1)}\t{t}\t0\t1\t4"
        )

    writer = threading.Thread(target=_write_to_fifo, args=(fifo_path, lines, 0.02))
    writer.start()
    await asyncio.to_thread(writer.join)
    await asyncio.sleep(0.3)

    reader_task.cancel()
    try:
        await reader_task
    except asyncio.CancelledError:
        pass

    # Linear data should compress significantly (start + end at minimum)
    assert len(reader.compressed_series) < 10
    assert len(reader.compressed_series) >= 2


@pytest.mark.asyncio
async def test_write_compressed(tmp_path: Path):
    """write_compressed should produce a valid TSV file."""
    from diracx.api.prmon_reader import PrmonFifoReader

    fifo_path = tmp_path / "prmon_fifo"
    os.mkfifo(fifo_path)

    reader = PrmonFifoReader(fifo_path)
    reader_task = asyncio.create_task(reader.run())
    await asyncio.sleep(0)

    writer = threading.Thread(
        target=_write_to_fifo,
        args=(fifo_path, [SAMPLE_HEADER, SAMPLE_ROW_1, SAMPLE_ROW_2]),
    )
    writer.start()
    await asyncio.to_thread(writer.join)
    await asyncio.sleep(0.2)

    reader_task.cancel()
    try:
        await reader_task
    except asyncio.CancelledError:
        pass

    output = tmp_path / "prmon_compressed.txt"
    reader.write_compressed(output)

    assert output.exists()
    lines = output.read_text().strip().splitlines()
    assert len(lines) >= 2  # header + at least one data row
    assert lines[0] == SAMPLE_HEADER


@pytest.mark.asyncio
async def test_write_compressed_empty(tmp_path: Path):
    """write_compressed should be a no-op when there is no data."""
    from diracx.api.prmon_reader import PrmonFifoReader

    fifo_path = tmp_path / "prmon_fifo"
    reader = PrmonFifoReader(fifo_path)

    output = tmp_path / "prmon_compressed.txt"
    reader.write_compressed(output)
    assert not output.exists()
