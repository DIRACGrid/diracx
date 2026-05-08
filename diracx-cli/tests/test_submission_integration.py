from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import yaml

from diracx.cli._submission.pipeline import submit_cwl


@staticmethod
def _cwl_workflow(tmp_path: Path) -> Path:
    """CWL with File input + string input."""
    cwl = {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "label": "integration-test",
        "hints": [
            {
                "class": "dirac:Job",
                "schema_version": "1.0",
                "type": "User",
                "input_sandbox": [{"source": "script"}],
            }
        ],
        "inputs": [
            {"id": "script", "type": "File"},
            {"id": "message", "type": "string"},
        ],
        "outputs": [],
        "baseCommand": ["python"],
        "$namespaces": {"dirac": "https://diracgrid.org/cwl#"},
    }
    f = tmp_path / "workflow.cwl"
    f.write_text(yaml.dump(cwl))
    return f


class TestIntegration:
    async def test_submit_with_local_file_sandbox(self, tmp_path):
        """Submit a job with a local File input → should upload sandbox and rewrite."""
        workflow = _cwl_workflow(tmp_path)
        local_script = tmp_path / "run.py"
        local_script.write_text("print('hello')")

        inputs_file = tmp_path / "inputs.yaml"
        inputs_file.write_text(
            yaml.dump(
                {
                    "script": {"class": "File", "path": str(local_script)},
                    "message": "integration test",
                }
            )
        )

        mock_client = AsyncMock()
        mock_client.jobs.submit_cwl_jobs = AsyncMock(
            return_value=[
                MagicMock(
                    job_id=2001, status="Submitting", minor_status="Initializing Job"
                )
            ]
        )

        fake_sb_ref = "SB:SandboxSE|/S3/bucket/sha256:abc123.tar.zst"

        with (
            patch(
                "diracx.cli._submission.pipeline.AsyncDiracClient"
            ) as mock_client_cls,
            patch(
                "diracx.api.jobs.create_sandbox",
                new_callable=AsyncMock,
                return_value=fake_sb_ref,
            ),
        ):
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await submit_cwl(
                workflow=workflow,
                input_files=[inputs_file],
                cli_args=[],
                range_spec=None,
                yes=True,
            )

        assert len(results) == 1
        call_body = mock_client.jobs.submit_cwl_jobs.call_args[0][0]
        submitted_inputs = call_body.inputs[0]
        assert submitted_inputs["script"]["location"] == f"{fake_sb_ref}#run.py"
        assert "path" not in submitted_inputs["script"]
        assert submitted_inputs["message"] == "integration test"

    async def test_submit_with_lfn_no_sandbox(self, tmp_path):
        """Submit with LFN input → no sandbox upload."""
        workflow = _cwl_workflow(tmp_path)

        inputs_file = tmp_path / "inputs.yaml"
        inputs_file.write_text(
            yaml.dump(
                {
                    "script": {"class": "File", "location": "LFN:/lhcb/scripts/run.py"},
                    "message": "lfn test",
                }
            )
        )

        mock_client = AsyncMock()
        mock_client.jobs.submit_cwl_jobs = AsyncMock(
            return_value=[
                MagicMock(
                    job_id=2002, status="Submitting", minor_status="Initializing Job"
                )
            ]
        )

        with patch(
            "diracx.cli._submission.pipeline.AsyncDiracClient"
        ) as mock_client_cls:
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await submit_cwl(
                workflow=workflow,
                input_files=[inputs_file],
                cli_args=[],
                range_spec=None,
                yes=True,
            )

        call_body = mock_client.jobs.submit_cwl_jobs.call_args[0][0]
        submitted_inputs = call_body.inputs[0]
        assert submitted_inputs["script"]["location"] == "LFN:/lhcb/scripts/run.py"

    async def test_multi_doc_yaml_parametric(self, tmp_path):
        """Multi-doc YAML creates multiple jobs sharing one sandbox."""
        workflow = _cwl_workflow(tmp_path)
        local_script = tmp_path / "run.py"
        local_script.write_text("print('hello')")

        sweep_file = tmp_path / "sweep.yaml"
        doc1 = {
            "script": {"class": "File", "path": str(local_script)},
            "message": "job 1",
        }
        doc2 = {
            "script": {"class": "File", "path": str(local_script)},
            "message": "job 2",
        }
        sweep_file.write_text(yaml.dump(doc1) + "---\n" + yaml.dump(doc2))

        mock_client = AsyncMock()
        mock_client.jobs.submit_cwl_jobs = AsyncMock(
            return_value=[
                MagicMock(
                    job_id=3001, status="Submitting", minor_status="Initializing Job"
                ),
                MagicMock(
                    job_id=3002, status="Submitting", minor_status="Initializing Job"
                ),
            ]
        )

        fake_sb_ref = "SB:SandboxSE|/S3/bucket/sha256:shared.tar.zst"

        with (
            patch(
                "diracx.cli._submission.pipeline.AsyncDiracClient"
            ) as mock_client_cls,
            patch(
                "diracx.api.jobs.create_sandbox",
                new_callable=AsyncMock,
                return_value=fake_sb_ref,
            ) as mock_create_sb,
        ):
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await submit_cwl(
                workflow=workflow,
                input_files=[sweep_file],
                cli_args=[],
                range_spec=None,
                yes=True,
            )

        # Sandbox uploaded once (both jobs share same local file)
        mock_create_sb.assert_called_once()

        # Two jobs submitted
        call_body = mock_client.jobs.submit_cwl_jobs.call_args[0][0]
        assert len(call_body.inputs) == 2
        assert call_body.inputs[0]["message"] == "job 1"
        assert call_body.inputs[1]["message"] == "job 2"
