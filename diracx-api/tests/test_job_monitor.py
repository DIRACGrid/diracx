"""Tests for JobMonitor."""

from __future__ import annotations

from collections import deque
from pathlib import Path

import pytest

from diracx.core.models.job import HeartbeatData


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
