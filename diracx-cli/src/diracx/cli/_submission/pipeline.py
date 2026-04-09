from __future__ import annotations

__all__ = ("submit_cwl",)

import logging
from pathlib import Path

import yaml

from diracx.client.aio import AsyncDiracClient  # type: ignore[attr-defined]
from diracx.client.models import CWLJobSubmission  # type: ignore[attr-defined]

from .confirm import build_summary, needs_confirmation, prompt_confirmation
from .inputs import parse_cli_args, parse_input_files, parse_range
from .sandbox import (
    group_jobs_by_sandbox,
    rewrite_sandbox_refs,
    scan_file_references,
)

logger = logging.getLogger(__name__)


def _extract_cwl_inputs(cwl: dict) -> list[dict]:
    """Extract input declarations from a parsed CWL document."""
    inputs = cwl.get("inputs", [])
    if isinstance(inputs, dict):
        return [{"id": k, **v} for k, v in inputs.items()]
    return inputs


def _extract_label(cwl: dict) -> str:
    """Extract the workflow label, falling back to 'unnamed'."""
    return cwl.get("label", "unnamed")


async def _upload_sandboxes(
    jobs: list[dict],
    sandbox_groups: list[tuple[frozenset[Path], list[int]]],
    client: AsyncDiracClient,
) -> list[dict]:
    """Upload sandboxes and rewrite File references in job inputs."""
    from diracx.api.jobs import create_sandbox

    pfn_map: dict[Path, str] = {}
    for file_set, _job_indices in sandbox_groups:
        paths = sorted(file_set)
        pfn = await create_sandbox(paths, client=client)
        for p in paths:
            pfn_map[p] = pfn

    return [rewrite_sandbox_refs(job, pfn_map) for job in jobs]


async def submit_cwl(
    *,
    workflow: Path,
    input_files: list[Path],
    cli_args: list[str],
    range_spec: str | None,
    yes: bool,
) -> list:
    """Shared CWL submission pipeline.

    1. Parse CWL and inputs
    2. Validate
    3. Process sandboxes
    4. Confirm if needed
    5. Submit via API
    6. Return results
    """
    # 1. Parse CWL
    cwl_yaml = workflow.read_text()
    cwl = yaml.safe_load(cwl_yaml)
    cwl_inputs = _extract_cwl_inputs(cwl)
    label = _extract_label(cwl)

    # 2. Parse inputs
    jobs: list[dict] = parse_input_files(input_files)

    # Merge CLI args if provided
    if cli_args:
        cli_input = parse_cli_args(cwl_inputs, cli_args)
        if jobs:
            jobs = [{**job, **cli_input} for job in jobs]
        else:
            jobs = [cli_input]

    # 3. Handle range — server-side expansion
    if range_spec:
        param, start, end, step = parse_range(range_spec)
        base_inputs = jobs[0] if jobs else None

        async with AsyncDiracClient() as client:
            body = CWLJobSubmission(
                workflow=cwl_yaml,
                range_param=param,
                range_start=start,
                range_end=end,
                range_step=step,
                base_inputs=base_inputs,
            )
            return await client.jobs.submit_cwl_jobs(body)

    # 4. Sandbox processing
    if not jobs:
        jobs = [{}]

    sandbox_groups = group_jobs_by_sandbox(jobs)

    # Determine source description for summary
    if len(input_files) > 0:
        source = f"{len(input_files)} input file(s)"
    else:
        source = "no inputs"

    # Count LFNs across all jobs
    total_lfns = 0
    for job in jobs:
        _, lfns = scan_file_references(job)
        total_lfns += len(lfns)

    # Compute sandbox sizes (approximate from local files)
    total_sandbox_bytes = 0
    for file_set, _ in sandbox_groups:
        for f in file_set:
            if f.exists():
                total_sandbox_bytes += f.stat().st_size

    # 5. Confirm if needed
    if needs_confirmation(len(jobs), yes=yes):
        summary = build_summary(
            workflow_name=label,
            workflow_path=str(workflow),
            num_jobs=len(jobs),
            source=source,
            num_unique_sandboxes=len(sandbox_groups),
            total_sandbox_bytes=total_sandbox_bytes,
            num_lfn_inputs=total_lfns,
        )
        if not prompt_confirmation(summary):
            raise SystemExit("Submission cancelled.")

    # 6. Upload sandboxes and submit
    async with AsyncDiracClient() as client:
        if sandbox_groups:
            jobs = await _upload_sandboxes(jobs, sandbox_groups, client)

        body = CWLJobSubmission(
            workflow=cwl_yaml,
            inputs=jobs if jobs != [{}] else [],
        )
        return await client.jobs.submit_cwl_jobs(body)
