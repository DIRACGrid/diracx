"""Tests for JobReport."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from diracx.api.job_report import JobReport
from diracx.client.models import HeartbeatData, JobCommand  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_send_heartbeat_returns_commands():
    """send_heartbeat should call add_heartbeat and return commands."""
    mock_client = MagicMock()
    kill_cmd = JobCommand(job_id=42, command="Kill", arguments=None)
    mock_client.jobs.add_heartbeat = AsyncMock(return_value=[kill_cmd])

    report = JobReport(job_id=42, source="JobWrapper", client=mock_client)
    metrics = HeartbeatData(
        cpu_consumed=10.5,
        wall_clock_time=60.0,
        memory_used=512.0,
    )
    commands = await report.send_heartbeat(metrics)

    mock_client.jobs.add_heartbeat.assert_called_once_with({"42": metrics})
    assert len(commands) == 1
    assert commands[0].command == "Kill"


@pytest.mark.asyncio
async def test_send_heartbeat_empty_commands():
    """send_heartbeat returns empty list when no commands."""
    mock_client = MagicMock()
    mock_client.jobs.add_heartbeat = AsyncMock(return_value=[])

    report = JobReport(job_id=42, source="JobWrapper", client=mock_client)
    metrics = HeartbeatData(wall_clock_time=30.0)
    commands = await report.send_heartbeat(metrics)

    assert commands == []


@pytest.mark.asyncio
async def test_send_heartbeat_propagates_error():
    """send_heartbeat should propagate API errors to the caller."""
    mock_client = MagicMock()
    mock_client.jobs.add_heartbeat = AsyncMock(side_effect=RuntimeError("API down"))

    report = JobReport(job_id=42, source="JobWrapper", client=mock_client)
    metrics = HeartbeatData(wall_clock_time=30.0)

    with pytest.raises(RuntimeError, match="API down"):
        await report.send_heartbeat(metrics)
