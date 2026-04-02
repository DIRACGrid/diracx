"""CWL workflow submission logic.

Handles parsing CWL + input YAMLs, extracting the dirac:Job hint,
translating to JDL (transition period), and storing workflow references.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, TypeAlias, Union

import yaml
from cwl_utils.parser import load_document_by_yaml
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    CUDARequirement,
    ExpressionTool,
    MPIRequirement,
    ResourceRequirement,
    Workflow,
)

from diracx.core.config import Config
from diracx.core.models.auth import UserInfo
from diracx.core.models.cwl import JobHint
from diracx.core.models.job import InsertedJob
from diracx.db.sql.job.db import JobDB
from diracx.db.sql.job_logging.db import JobLoggingDB

from .submission import submit_jdl_jobs

logger = logging.getLogger(__name__)

SUPPORTED_SCHEMA_VERSIONS = {"1.0"}

CWLTask: TypeAlias = Union[CommandLineTool, Workflow, ExpressionTool]


def compute_workflow_id(cwl_yaml: str) -> str:
    """Content-address a CWL workflow by its SHA-256 hash."""
    return hashlib.sha256(cwl_yaml.encode()).hexdigest()


def parse_cwl(cwl_yaml: str) -> CWLTask:
    """Parse a CWL YAML string into a cwl_utils object."""
    doc = yaml.safe_load(cwl_yaml)
    return load_document_by_yaml(doc, uri="workflow.cwl")


def extract_job_hint(task: CWLTask) -> JobHint:
    """Extract and validate the dirac:Job hint from a CWL task."""
    job_hint = JobHint.from_cwl(task)

    if job_hint.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(
            f"Unsupported dirac:Job schema_version '{job_hint.schema_version}'. "
            f"Supported: {SUPPORTED_SCHEMA_VERSIONS}"
        )

    return job_hint


def _extract_id(cwl_id: str) -> str:
    """Extract short ID from CWL full URI (e.g., 'file.cwl#input1' -> 'input1')."""
    return cwl_id.split("#")[-1].split("/")[-1]


def _validate_cwl_id(
    source: str,
    cwl_ids: dict[str, Any],
    direction: str,
    allowed_types: list[str],
) -> None:
    """Validate that a source ID exists in the CWL task's inputs/outputs."""
    if source not in cwl_ids:
        available = ", ".join(sorted(cwl_ids.keys()))
        raise ValueError(
            f"dirac:Job references {direction} '{source}' but CWL task "
            f"only has: [{available}]"
        )


def cwl_to_jdl(
    task: CWLTask,
    job_hint: JobHint,
    input_params: dict | None,
) -> str:
    """Convert a CWL task with dirac:Job hint into a JDL string.

    This is a transition-period function -- once JDL is retired,
    job attributes are populated directly from the hint + CWL.
    """
    jdl_fields: dict[str, Any] = {
        "Executable": "dirac-cwl-exec",
        "JobType": job_hint.type,
        "Priority": job_hint.priority,
        "LogLevel": job_hint.log_level,
    }

    if job_hint.cpu_work:
        jdl_fields["CPUTime"] = job_hint.cpu_work
    if job_hint.platform:
        jdl_fields["Platform"] = job_hint.platform

    # Derive JobName from CWL label/id
    task_label = getattr(task, "label", None)
    task_id = getattr(task, "id", None)
    if task_label:
        jdl_fields["JobName"] = task_label
    elif task_id and task_id != ".":
        jdl_fields["JobName"] = task_id.split("#")[-1].split("/")[-1]

    # Extract from CWL requirements (standard CWL, not dirac:Job)
    tags = set(job_hint.tags or [])
    for req in getattr(task, "requirements", None) or []:
        if isinstance(req, ResourceRequirement):
            if req.coresMin:
                jdl_fields["MinNumberOfProcessors"] = int(req.coresMin)
            if req.coresMax:
                jdl_fields["MaxNumberOfProcessors"] = int(req.coresMax)
            if req.ramMin:
                jdl_fields["MinRAM"] = int(req.ramMin)
            if req.ramMax:
                jdl_fields["MaxRAM"] = int(req.ramMax)
        elif isinstance(req, CUDARequirement):
            tags.add("GPU")
        elif isinstance(req, MPIRequirement):
            raise NotImplementedError(
                "MPIRequirement is not yet supported for DIRAC CWL jobs"
            )

    # Auto-derive processor tags
    min_proc = jdl_fields.get("MinNumberOfProcessors", 1)
    max_proc = jdl_fields.get("MaxNumberOfProcessors")
    if min_proc and min_proc > 1:
        tags.add("MultiProcessor")
    if min_proc and max_proc and min_proc == max_proc:
        tags.add(f"{min_proc}Processors")

    if tags:
        jdl_fields["Tags"] = list(tags)

    # Sites
    if job_hint.sites:
        jdl_fields["Site"] = job_hint.sites
    if job_hint.banned_sites:
        jdl_fields["BannedSites"] = job_hint.banned_sites

    if job_hint.group:
        jdl_fields["JobGroup"] = job_hint.group

    # Resolve I/O from CWL input/output source IDs
    cwl_input_ids = {
        _extract_id(inp.id): inp for inp in (getattr(task, "inputs", None) or [])
    }
    cwl_output_ids = {
        _extract_id(out.id): out for out in (getattr(task, "outputs", None) or [])
    }

    # InputSandbox
    if job_hint.input_sandbox:
        sandbox_files = []
        for ref in job_hint.input_sandbox:
            _validate_cwl_id(ref.source, cwl_input_ids, "input", ["File", "File[]"])
            if input_params and ref.source in input_params:
                val = input_params[ref.source]
                if isinstance(val, dict) and "path" in val:
                    sandbox_files.append(val["path"])
                elif isinstance(val, list):
                    sandbox_files.extend(
                        item["path"]
                        for item in val
                        if isinstance(item, dict) and "path" in item
                    )
        if sandbox_files:
            jdl_fields["InputSandbox"] = sandbox_files

    # InputData
    if job_hint.input_data:
        lfns = []
        for ref in job_hint.input_data:
            _validate_cwl_id(ref.source, cwl_input_ids, "input", ["File", "File[]"])
            if input_params and ref.source in input_params:
                val = input_params[ref.source]
                if isinstance(val, dict) and "path" in val:
                    lfns.append(val["path"])
                elif isinstance(val, list):
                    lfns.extend(
                        item["path"]
                        for item in val
                        if isinstance(item, dict) and "path" in item
                    )
        if lfns:
            jdl_fields["InputData"] = lfns

    # OutputSandbox
    if job_hint.output_sandbox:
        sandbox_outputs = []
        for ref in job_hint.output_sandbox:
            _validate_cwl_id(ref.source, cwl_output_ids, "output", ["File", "File[]"])
            out = cwl_output_ids[ref.source]
            if hasattr(out, "outputBinding") and out.outputBinding:
                sandbox_outputs.append(out.outputBinding.glob)
        if sandbox_outputs:
            jdl_fields["OutputSandbox"] = sandbox_outputs

    # OutputData (per-output SE and path)
    if job_hint.output_data:
        output_files = []
        all_ses = set()
        for entry in job_hint.output_data:
            _validate_cwl_id(entry.source, cwl_output_ids, "output", ["File", "File[]"])
            out = cwl_output_ids[entry.source]
            if hasattr(out, "outputBinding") and out.outputBinding:
                output_files.append(out.outputBinding.glob)
            all_ses.update(entry.output_se)
        if output_files:
            jdl_fields["OutputData"] = output_files
            jdl_fields["OutputPath"] = job_hint.output_data[0].output_path
            jdl_fields["OutputSE"] = list(all_ses)

    return _format_as_jdl(jdl_fields)


def _format_as_jdl(fields: dict[str, Any]) -> str:
    """Format a dict of fields as a JDL string."""
    lines = []
    for key, value in fields.items():
        if isinstance(value, list):
            items = ", ".join(f'"{v}"' for v in value)
            lines.append(f"  {key} = {{{items}}};")
        elif isinstance(value, int):
            lines.append(f"  {key} = {value};")
        elif isinstance(value, str):
            lines.append(f'  {key} = "{value}";')
        else:
            lines.append(f'  {key} = "{value}";')
    return "[\n" + "\n".join(lines) + "\n]"


async def submit_cwl_jobs(
    cwl_yaml: str,
    input_yamls: list[dict | None],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: UserInfo,
    config: Config,
) -> list[InsertedJob]:
    """Submit CWL jobs: store workflow once, create one job per input YAML."""
    workflow_id = compute_workflow_id(cwl_yaml)

    # INSERT IF NOT EXISTS — idempotent, content-addressed
    await job_db.insert_workflow(workflow_id, cwl_yaml, persistent=False)

    task = parse_cwl(cwl_yaml)
    job_hint = extract_job_hint(task)

    inserted: list[InsertedJob] = []
    for input_params in input_yamls:
        # Generate JDL for transition period
        jdl = cwl_to_jdl(task, job_hint, input_params)

        # Submit via existing pipeline
        jobs = await submit_jdl_jobs(
            [jdl],
            job_db=job_db,
            job_logging_db=job_logging_db,
            user_info=user_info,
            config=config,
        )

        # Set workflow reference + immutable params on job row
        for job in jobs:
            await job_db.set_workflow_ref(
                job.JobID,
                workflow_id=workflow_id,
                workflow_params=input_params,
            )

        inserted.extend(jobs)

    return inserted
