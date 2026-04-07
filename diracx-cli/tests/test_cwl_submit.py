from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from typer.testing import CliRunner

from diracx.cli.cwl import app as cwl_app

runner = CliRunner()


@pytest.fixture
def cwl_file(tmp_path: Path) -> Path:
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
        "inputs": [{"id": "message", "type": "string"}],
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


class TestCWLSubmitCommand:
    def test_basic_submit(self, cwl_file, inputs_file):
        with patch(
            "diracx.cli.cwl.submit.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [MagicMock(job_id=1001, status="Submitting")]
            result = runner.invoke(
                cwl_app,
                ["submit", str(cwl_file), str(inputs_file), "-y"],
            )
        assert result.exit_code == 0, result.output
        assert "1001" in result.output
        mock_submit.assert_called_once()
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["workflow"] == cwl_file
        assert call_kwargs["input_files"] == [inputs_file]
        assert call_kwargs["yes"] is True

    def test_range_submit(self, cwl_file):
        with patch(
            "diracx.cli.cwl.submit.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [
                MagicMock(job_id=i, status="Submitting") for i in range(10)
            ]
            result = runner.invoke(
                cwl_app,
                ["submit", str(cwl_file), "--range", "message=0:10", "-y"],
            )
        assert result.exit_code == 0, result.output
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["range_spec"] == "message=0:10"

    def test_cli_args_after_separator(self, cwl_file):
        with patch(
            "diracx.cli.cwl.submit.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [MagicMock(job_id=1001, status="Submitting")]
            result = runner.invoke(
                cwl_app,
                ["submit", str(cwl_file), "-y", "--", "--message", "hello"],
            )
        assert result.exit_code == 0, result.output
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["cli_args"] == ["--message", "hello"]
