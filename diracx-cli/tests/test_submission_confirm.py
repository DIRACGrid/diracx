# diracx-cli/tests/test_submission_confirm.py
from __future__ import annotations

from diracx.cli._submission.confirm import (
    build_summary,
    needs_confirmation,
)


class TestNeedsConfirmation:
    def test_under_100_jobs(self):
        assert needs_confirmation(num_jobs=50) is False

    def test_exactly_100_jobs(self):
        assert needs_confirmation(num_jobs=100) is False

    def test_over_100_jobs(self):
        assert needs_confirmation(num_jobs=101) is True

    def test_skip_with_yes(self):
        assert needs_confirmation(num_jobs=1000, yes=True) is False


class TestBuildSummary:
    def test_basic_summary(self):
        summary = build_summary(
            workflow_name="hello-world",
            workflow_path="workflow.cwl",
            num_jobs=1000,
            source="--range seed=0:1000",
            num_unique_sandboxes=1,
            total_sandbox_bytes=45 * 1024 * 1024,
            num_lfn_inputs=0,
        )
        assert "workflow.cwl" in summary
        assert "hello-world" in summary
        assert "1,000" in summary or "1000" in summary
        assert "--range seed=0:1000" in summary

    def test_with_lfn_inputs(self):
        summary = build_summary(
            workflow_name="lfn-job",
            workflow_path="workflow.cwl",
            num_jobs=500,
            source="--range seed=0:500",
            num_unique_sandboxes=0,
            total_sandbox_bytes=0,
            num_lfn_inputs=1000,
        )
        assert "1,000" in summary or "1000" in summary
        assert "LFN" in summary

    def test_no_sandboxes_no_lfns(self):
        summary = build_summary(
            workflow_name="simple",
            workflow_path="workflow.cwl",
            num_jobs=200,
            source="--range seed=0:200",
            num_unique_sandboxes=0,
            total_sandbox_bytes=0,
            num_lfn_inputs=0,
        )
        assert "Sandbox" not in summary
        assert "LFN" not in summary
