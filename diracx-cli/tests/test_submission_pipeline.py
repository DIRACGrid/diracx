from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from diracx.cli._submission.pipeline import submit_cwl


@pytest.fixture
def cwl_workflow(tmp_path: Path) -> Path:
    """Create a minimal CWL workflow file."""
    cwl = {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "label": "test-tool",
        "hints": [
            {
                "class": "dirac:Job",
                "schema_version": "1.0",
                "type": "User",
            }
        ],
        "inputs": [
            {"id": "message", "type": "string"},
        ],
        "outputs": [],
        "baseCommand": ["echo"],
        "$namespaces": {"dirac": "https://diracgrid.org/cwl#"},
    }
    f = tmp_path / "workflow.cwl"
    f.write_text(yaml.dump(cwl))
    return f


@pytest.fixture
def inputs_file(tmp_path: Path) -> Path:
    f = tmp_path / "inputs.yaml"
    f.write_text("message: hello\n")
    return f


@pytest.fixture
def mock_client():
    """Mock AsyncDiracClient for submission."""
    client = AsyncMock()
    client.jobs.submit_cwl_jobs = AsyncMock(
        return_value=[
            MagicMock(job_id=1001, status="Submitting", minor_status="Initializing Job")
        ]
    )
    return client


class TestSubmitCWL:
    @pytest.mark.asyncio
    async def test_single_job_submission(self, cwl_workflow, inputs_file, mock_client):
        with patch(
            "diracx.cli._submission.pipeline.AsyncDiracClient"
        ) as mock_client_cls:
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await submit_cwl(
                workflow=cwl_workflow,
                input_files=[inputs_file],
                cli_args=[],
                range_spec=None,
                yes=True,
            )

        assert len(result) == 1
        assert result[0].job_id == 1001
        mock_client.jobs.submit_cwl_jobs.assert_called_once()
        call_body = mock_client.jobs.submit_cwl_jobs.call_args[0][0]
        assert "cwlVersion" in call_body.workflow
        assert call_body.inputs == [{"message": "hello"}]

    @pytest.mark.asyncio
    async def test_no_inputs_submission(self, cwl_workflow, mock_client):
        with patch(
            "diracx.cli._submission.pipeline.AsyncDiracClient"
        ) as mock_client_cls:
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await submit_cwl(
                workflow=cwl_workflow,
                input_files=[],
                cli_args=[],
                range_spec=None,
                yes=True,
            )

        call_body = mock_client.jobs.submit_cwl_jobs.call_args[0][0]
        assert call_body.inputs == []

    @pytest.mark.skip(reason="Range fields added in Task 8")
    @pytest.mark.asyncio
    async def test_range_submission(self, cwl_workflow, mock_client):
        pass
