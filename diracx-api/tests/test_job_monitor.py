"""Tests for JobMonitor."""

from __future__ import annotations

import asyncio
import os
from collections import deque
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from diracx.core.models.job import HeartbeatData, JobCommand


@pytest.fixture
def prmon_tsv(tmp_path: Path) -> Path:
    """Create a prmon TSV file with two sample rows."""
    tsv = tmp_path / "prmon.txt"
    header = (
        "Time\twtime\tpss\trss\tswap\tvmem\t"
        "rchar\tread_bytes\twchar\twrite_bytes\t"
        "rx_bytes\trx_packets\ttx_bytes\ttx_packets\t"
        "stime\tutime\tnprocs\tnthreads"
    )
    row1 = "1713000030\t30\t15000\t20000\t0\t50000\t100\t200\t300\t400\t0\t0\t0\t0\t2\t8\t1\t4"
    row2 = "1713000060\t60\t18000\t24000\t0\t55000\t200\t400\t600\t800\t0\t0\t0\t0\t4\t16\t1\t4"
    tsv.write_text(f"{header}\n{row1}\n{row2}\n")
    return tsv


def test_parse_prmon_tsv(prmon_tsv: Path):
    """parse_prmon_tsv should return latest row as a dict with correct types."""
    from diracx.api.job_monitor import parse_prmon_tsv

    row = parse_prmon_tsv(prmon_tsv)
    assert row is not None
    assert row["wtime"] == 60
    assert row["pss"] == 18000  # KB
    assert row["rss"] == 24000  # KB
    assert row["vmem"] == 55000  # KB
    assert row["utime"] == 16  # seconds
    assert row["stime"] == 4  # seconds


def test_parse_prmon_tsv_missing_file(tmp_path: Path):
    """parse_prmon_tsv returns None for a missing file."""
    from diracx.api.job_monitor import parse_prmon_tsv

    result = parse_prmon_tsv(tmp_path / "nonexistent.txt")
    assert result is None


def test_parse_prmon_tsv_header_only(tmp_path: Path):
    """parse_prmon_tsv returns None if the file has only a header."""
    from diracx.api.job_monitor import parse_prmon_tsv

    tsv = tmp_path / "prmon.txt"
    tsv.write_text(
        "Time\twtime\tpss\trss\tswap\tvmem\t"
        "rchar\tread_bytes\twchar\twrite_bytes\t"
        "rx_bytes\trx_packets\ttx_bytes\ttx_packets\t"
        "stime\tutime\tnprocs\tnthreads\n"
    )
    assert parse_prmon_tsv(tsv) is None


def test_build_heartbeat_data(prmon_tsv: Path, tmp_path: Path):
    """build_heartbeat_data should map prmon metrics to HeartbeatData fields."""
    from diracx.api.job_monitor import build_heartbeat_data, parse_prmon_tsv

    row = parse_prmon_tsv(prmon_tsv)
    assert row is not None
    data = build_heartbeat_data(
        prmon_row=row,
        job_path=tmp_path,
        peek_content="last lines of output",
    )
    assert isinstance(data, HeartbeatData)
    # CPU = utime + stime = 16 + 4 = 20 seconds
    assert data.CPUConsumed == 20.0
    # Memory = pss / 1024 = 18000 / 1024
    assert data.MemoryUsed is not None
    assert abs(data.MemoryUsed - 18000 / 1024) < 0.01
    # Vsize = vmem / 1024 = 55000 / 1024
    assert data.Vsize is not None
    assert abs(data.Vsize - 55000 / 1024) < 0.01
    # WallClockTime = wtime = 60 seconds
    assert data.WallClockTime == 60.0
    # AvailableDiskSpace should be set (from os.statvfs)
    assert data.AvailableDiskSpace is not None
    assert data.AvailableDiskSpace > 0
    # Peek content
    assert data.StandardOutput == "last lines of output"


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
async def test_job_monitor_sends_heartbeat(tmp_path: Path, mock_job_report, prmon_tsv):
    """JobMonitor.run should send at least one heartbeat before being cancelled."""
    from diracx.api.job_monitor import JobMonitor

    monitor = JobMonitor(
        pid=os.getpid(),
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(),
        heartbeat_interval=0.1,  # fast for testing
        prmon_tsv_path=prmon_tsv,
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
    assert data.CPUConsumed == 20.0  # from prmon_tsv fixture


@pytest.mark.asyncio
async def test_job_monitor_handles_kill_command(
    tmp_path: Path, mock_job_report, prmon_tsv
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
        prmon_tsv_path=prmon_tsv,
        kill_grace_period=0.1,  # fast for testing
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
async def test_send_final_heartbeat(tmp_path: Path, mock_job_report, prmon_tsv):
    """send_final_heartbeat should send one heartbeat with prmon exit data."""
    from diracx.api.job_monitor import send_final_heartbeat

    await send_final_heartbeat(
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(["INFO [job test] completed success"]),
        prmon_tsv_path=prmon_tsv,
    )

    assert mock_job_report.send_heartbeat.call_count == 1
    data = mock_job_report.send_heartbeat.call_args[0][0]
    assert isinstance(data, HeartbeatData)
    assert data.CPUConsumed == 20.0


@pytest.mark.asyncio
async def test_send_final_heartbeat_no_prmon(tmp_path: Path, mock_job_report):
    """send_final_heartbeat should not crash if prmon data is missing."""
    from diracx.api.job_monitor import send_final_heartbeat

    await send_final_heartbeat(
        job_path=tmp_path,
        job_report=mock_job_report,
        cwltool_stderr=deque(),
        prmon_tsv_path=tmp_path / "nonexistent.txt",
    )

    assert mock_job_report.send_heartbeat.call_count == 0
