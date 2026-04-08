from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from typer.testing import CliRunner

from diracx.cli.job import app as job_app

runner = CliRunner()


@staticmethod
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


class TestCWLSubmitCommand:
    def test_basic_submit(self, tmp_path):
        wf = cwl_file(tmp_path)
        inputs_f = tmp_path / "inputs.yaml"
        inputs_f.write_text("message: hello\n")

        with patch(
            "diracx.cli.job.submit.cwl.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [MagicMock(job_id=1001, status="Submitting")]
            result = runner.invoke(
                job_app,
                ["submit", "cwl", str(wf), str(inputs_f), "-y"],
            )
        assert result.exit_code == 0, result.output
        assert "1001" in result.output
        mock_submit.assert_called_once()
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["workflow"] == wf
        assert call_kwargs["input_files"] == [inputs_f]
        assert call_kwargs["yes"] is True

    def test_range_submit(self, tmp_path):
        wf = cwl_file(tmp_path)
        with patch(
            "diracx.cli.job.submit.cwl.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [
                MagicMock(job_id=i, status="Submitting") for i in range(10)
            ]
            result = runner.invoke(
                job_app,
                ["submit", "cwl", str(wf), "--range", "message=0:10", "-y"],
            )
        assert result.exit_code == 0, result.output
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["range_spec"] == "message=0:10"

    def test_cli_args_after_separator(self, tmp_path):
        wf = cwl_file(tmp_path)
        with patch(
            "diracx.cli.job.submit.cwl.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [MagicMock(job_id=1001, status="Submitting")]
            result = runner.invoke(
                job_app,
                ["submit", "cwl", str(wf), "-y", "--", "--message", "hello"],
            )
        assert result.exit_code == 0, result.output
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["cli_args"] == ["--message", "hello"]
