"""Tests for JobMonitor."""

from __future__ import annotations

import asyncio
import os
from collections import deque
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from diracx.client.models import HeartbeatData, JobCommand  # type: ignore[attr-defined]


@pytest.fixture
def mock_fifo_reader():
    """Create a mock PrmonFifoReader with sample data pre-loaded."""
    reader = MagicMock()
    reader.latest_row = {
        "Time": 1713000060,
        "wtime": 60,
        "pss": 18000,
        "rss": 24000,
        "swap": 0,
        "vmem": 55000,
        "rchar": 200,
        "read_bytes": 400,
        "wchar": 600,
        "write_bytes": 800,
        "rx_bytes": 0,
        "rx_packets": 0,
        "tx_bytes": 0,
        "tx_packets": 0,
        "stime": 4,
        "utime": 16,
        "nprocs": 1,
        "nthreads": 4,
    }
    reader.compressed_series = [reader.latest_row]
    return reader


def test_build_heartbeat_data(tmp_path: Path):
    """build_heartbeat_data should map prmon metrics to HeartbeatData fields."""
    from diracx.api.job_monitor import build_heartbeat_data

    row = {
        "Time": 1713000060,
        "wtime": 60,
        "pss": 18000,
        "rss": 24000,
        "swap": 0,
        "vmem": 55000,
        "stime": 4,
        "utime": 16,
        "nprocs": 1,
        "nthreads": 4,
    }
    data = build_heartbeat_data(
        prmon_row=row,
        job_path=tmp_path,
        peek_content="last lines of output",
    )
    assert isinstance(data, HeartbeatData)
    # CPU = utime + stime = 16 + 4 = 20 seconds
    assert data.cpu_consumed == 20.0
    # Memory = pss / 1024 = 18000 / 1024
    assert data.memory_used is not None
    assert abs(data.memory_used - 18000 / 1024) < 0.01
    # Vsize = vmem / 1024 = 55000 / 1024
    assert data.vsize is not None
    assert abs(data.vsize - 55000 / 1024) < 0.01
    # WallClockTime = wtime = 60 seconds
    assert data.wall_clock_time == 60.0
    # AvailableDiskSpace should be set (from os.statvfs)
    assert data.available_disk_space is not None
    assert data.available_disk_space > 0
    # Peek content
    assert data.standard_output == "last lines of output"


# --- Task 3: Peek content tests ---


def test_build_peek_content():
    """build_peek_content should return cwltool stderr lines."""
    from diracx.api.job_monitor import build_peek_content

    cwltool_deque: deque[str] = deque(
        ["INFO [job test] starting", "INFO [job test] completed success"],
        maxlen=100,
    )

    content = build_peek_content(cwltool_deque)

    assert "[job test] starting" in content
    assert "[job test] completed success" in content


def test_build_peek_content_empty():
    """build_peek_content should handle empty deque gracefully."""
    from diracx.api.job_monitor import build_peek_content

    content = build_peek_content(deque())
    assert isinstance(content, str)


def test_build_peek_content_truncates():
    """build_peek_content should only include the last N lines."""
    from diracx.api.job_monitor import build_peek_content

    # Fill deque with 1000 lines — should only get last 800
    lines = deque((f"line-{i}" for i in range(1000)), maxlen=1000)

    content = build_peek_content(lines)
    assert "line-199" not in content
    assert "line-200" in content
    assert "line-999" in content


# --- Task 4: Stall detection tests ---


def test_stall_detector_not_stalled():
    """StallDetector should not trigger when CPU ratio is healthy."""
    from diracx.api.job_monitor import StallDetector

    detector = StallDetector(window_seconds=300, threshold=0.05)
    # Simulate 6 samples at 60s intervals — 50% CPU utilisation
    for i in range(1, 7):
        assert detector.check(cpu_seconds=30.0 * i, wall_seconds=60.0 * i) is False


def test_stall_detector_stalled():
    """StallDetector should trigger when CPU ratio stays below threshold."""
    from diracx.api.job_monitor import StallDetector

    detector = StallDetector(window_seconds=300, threshold=0.05)
    # Simulate 6 samples at 60s intervals — 1% CPU utilisation
    for i in range(1, 6):
        detector.check(cpu_seconds=0.6 * i, wall_seconds=60.0 * i)
    # At i=5, wall=300s which fills the window. Next sample should trigger.
    assert detector.check(cpu_seconds=3.6, wall_seconds=360.0) is True


def test_stall_detector_ignores_early_samples():
    """StallDetector should not trigger before the window is filled."""
    from diracx.api.job_monitor import StallDetector

    detector = StallDetector(window_seconds=1800, threshold=0.05)
    # Single sample with zero CPU — window not yet filled
    assert detector.check(cpu_seconds=0.0, wall_seconds=60.0) is False


# --- Task 5: JobMonitor tests ---


@pytest.fixture
def mock_job_report():
    """Create a mock JobReport with send_heartbeat."""
    report = MagicMock()
    report.send_heartbeat = AsyncMock(return_value=[])
    report.set_job_status = MagicMock()
    report.commit = AsyncMock()
    return report


@pytest.mark.asyncio
async def test_job_monitor_sends_heartbeat(
    tmp_path: Path, mock_job_report, mock_fifo_reader
):
    """JobMonitor.run should send at least one heartbeat before being cancelled."""
    from diracx.api.job_monitor import JobMonitor

    monitor = JobMonitor(
        pid=os.getpid(),
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(),
        heartbeat_interval=0.1,
        fifo_reader=mock_fifo_reader,
    )

    task = asyncio.create_task(monitor.run())
    await asyncio.sleep(0.3)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert mock_job_report.send_heartbeat.call_count >= 1
    call_args = mock_job_report.send_heartbeat.call_args
    data = call_args[0][0]
    assert isinstance(data, HeartbeatData)
    assert data.cpu_consumed == 20.0


@pytest.mark.asyncio
async def test_job_monitor_handles_kill_command(
    tmp_path: Path, mock_job_report, mock_fifo_reader
):
    """JobMonitor should raise KillCommandReceived when server sends Kill."""
    import signal

    from diracx.api.job_monitor import JobMonitor, KillCommandReceived

    kill_cmd = JobCommand(job_id=42, command="Kill")
    mock_job_report.send_heartbeat = AsyncMock(return_value=[kill_cmd])

    monitor = JobMonitor(
        pid=os.getpid(),
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(),
        heartbeat_interval=0.1,
        fifo_reader=mock_fifo_reader,
        kill_grace_period=0.1,
    )

    killpg_calls: list[tuple[int, int]] = []

    def mock_killpg(pgid, sig):
        killpg_calls.append((pgid, sig))

    import diracx.api.job_monitor as _jm_mod

    original_killpg = _jm_mod.os.killpg
    _jm_mod.os.killpg = mock_killpg  # type: ignore[attr-defined]
    try:
        with pytest.raises(KillCommandReceived):
            await asyncio.wait_for(monitor.run(), timeout=5.0)

        assert len(killpg_calls) == 2
        assert killpg_calls[0][1] == signal.SIGTERM
        assert killpg_calls[1][1] == signal.SIGKILL
    finally:
        _jm_mod.os.killpg = original_killpg  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_send_final_heartbeat(tmp_path: Path, mock_job_report, mock_fifo_reader):
    """send_final_heartbeat should send one heartbeat with prmon exit data."""
    from diracx.api.job_monitor import send_final_heartbeat

    await send_final_heartbeat(
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(["INFO [job test] completed success"]),
        fifo_reader=mock_fifo_reader,
    )

    assert mock_job_report.send_heartbeat.call_count == 1
    data = mock_job_report.send_heartbeat.call_args[0][0]
    assert isinstance(data, HeartbeatData)
    assert data.cpu_consumed == 20.0


@pytest.mark.asyncio
async def test_send_final_heartbeat_no_prmon(tmp_path: Path, mock_job_report):
    """send_final_heartbeat should not crash if there is no reader."""
    from diracx.api.job_monitor import send_final_heartbeat

    await send_final_heartbeat(
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(),
        fifo_reader=None,
    )

    assert mock_job_report.send_heartbeat.call_count == 0


@pytest.mark.asyncio
async def test_send_final_heartbeat_reader_no_data(tmp_path: Path, mock_job_report):
    """send_final_heartbeat should skip when reader has no data yet."""
    from diracx.api.job_monitor import send_final_heartbeat

    reader = MagicMock()
    reader.latest_row = None

    await send_final_heartbeat(
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(),
        fifo_reader=reader,
    )

    assert mock_job_report.send_heartbeat.call_count == 0
