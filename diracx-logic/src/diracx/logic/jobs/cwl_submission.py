"""CWL workflow submission logic.

Handles parsing CWL + input YAMLs, extracting the dirac:Job hint,
translating to JDL (transition period), and storing workflow references.
"""

from __future__ import annotations

import hashlib
import json
import logging
from enum import Enum
from typing import Any, TypeAlias, Union

import yaml
from cwl_utils.parser import load_document_by_yaml
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
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

CWLTask: TypeAlias = Union[CommandLineTool, Workflow, ExpressionTool]


class RequirementDisposition(Enum):
    """How each CWL Requirement class is handled at submission."""

    PASS_THROUGH = "pass_through"  # noqa: S105
    SUPPORTED = "supported"
    REJECTED = "rejected"


REQUIREMENT_WHITELIST: dict[str, RequirementDisposition] = {
    # Pass-through: execution-only, no matcher impact
    "InlineJavascriptRequirement": RequirementDisposition.PASS_THROUGH,
    "SchemaDefRequirement": RequirementDisposition.PASS_THROUGH,
    "InitialWorkDirRequirement": RequirementDisposition.PASS_THROUGH,
    "EnvVarRequirement": RequirementDisposition.PASS_THROUGH,
    "ShellCommandRequirement": RequirementDisposition.PASS_THROUGH,
    "LoadListingRequirement": RequirementDisposition.PASS_THROUGH,
    "InplaceUpdateRequirement": RequirementDisposition.PASS_THROUGH,
    "WorkReuse": RequirementDisposition.PASS_THROUGH,
    "NetworkAccess": RequirementDisposition.PASS_THROUGH,
    "SubworkflowFeatureRequirement": RequirementDisposition.PASS_THROUGH,
    "ScatterFeatureRequirement": RequirementDisposition.PASS_THROUGH,
    "MultipleInputFeatureRequirement": RequirementDisposition.PASS_THROUGH,
    "StepInputExpressionRequirement": RequirementDisposition.PASS_THROUGH,
    # Rejected
    "DockerRequirement": RequirementDisposition.REJECTED,
    "MPIRequirement": RequirementDisposition.REJECTED,
    "SoftwareRequirement": RequirementDisposition.REJECTED,
}


def _get_requirement_class_name(req) -> str:
    """Extract the class name from a CWL requirement object or dict."""
    if isinstance(req, dict):
        return req.get("class", "")
    return type(req).__name__


def validate_requirements(task: CWLTask) -> None:
    """Check all CWL requirements and hints against the whitelist.

    Raises ValueError if any requirement is rejected or unknown.
    """
    all_reqs: list[object] = []
    for attr in ("requirements", "hints"):
        items = getattr(task, attr, None) or []
        all_reqs.extend(items)

    for req in all_reqs:
        class_name = _get_requirement_class_name(req)

        # Skip the dirac:Job hint — it's ours, not a CWL requirement
        if class_name in ("dirac:Job", ""):
            continue

        disposition = REQUIREMENT_WHITELIST.get(class_name)

        if disposition is None:
            raise ValueError(
                f"CWL Requirement '{class_name}' is not supported. "
                f"Supported requirements: {sorted(REQUIREMENT_WHITELIST.keys())}"
            )

        if disposition == RequirementDisposition.REJECTED:
            raise ValueError(
                f"CWL Requirement '{class_name}' is not supported for "
                f"DIRAC CWL jobs and cannot be used."
            )


def build_matcher_docs(task: CWLTask, job_hint: JobHint) -> list[dict]:
    """Build matcher specification documents from the hint and CWL requirements.

    - Starts from job_hint.matcher (or [{}] if empty/absent).
    - Broadcasts supported CWL Requirement fields into every doc.
    - Detects conflicts between CWL Requirements and matcher doc values.
    """
    docs = [dict(d) for d in job_hint.matcher] if job_hint.matcher else [{}]

    # Currently no SUPPORTED requirements — this is the extension point.
    # When a requirement is promoted to SUPPORTED, add its broadcast logic here.

    return docs


def compute_workflow_id(cwl_yaml: str) -> str:
    """Content-address a CWL workflow by its SHA-256 hash.

    The YAML is parsed and re-serialized as sorted JSON before hashing,
    so whitespace, comments, and key ordering differences don't produce
    distinct workflow IDs.
    """
    canonical = json.dumps(yaml.safe_load(cwl_yaml), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def parse_cwl(cwl_yaml: str) -> CWLTask:
    """Parse a CWL YAML string into a cwl_utils object."""
    doc = yaml.safe_load(cwl_yaml)
    return load_document_by_yaml(doc, uri="workflow.cwl")


def extract_job_hint(task: CWLTask) -> JobHint:
    """Extract the dirac:Job hint from a CWL task, or return defaults if absent."""
    return JobHint.from_cwl(task)


_URI_PREFIXES = ("LFN:", "SB:")


def _get_file_ref(val: dict) -> str:
    """Return the file reference from a CWL File dict, preferring ``location``."""
    return val.get("location") or val.get("path", "")


def _validate_file_inputs(input_params: dict | None) -> None:
    """Reject CWL File objects that put URI schemes in ``path`` instead of ``location``."""
    if not input_params:
        return
    for key, value in input_params.items():
        items = value if isinstance(value, list) else [value]
        for item in items:
            if isinstance(item, dict) and item.get("class") == "File":
                path_val = item.get("path", "")
                if isinstance(path_val, str) and path_val.startswith(_URI_PREFIXES):
                    raise ValueError(
                        f"CWL File input '{key}' has a URI scheme in 'path' "
                        f"({path_val!r}). Use 'location' for LFN: and SB: "
                        f"references — 'path' is reserved for local "
                        f"filesystem paths."
                    )


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


def expand_range_inputs(
    *,
    range_param: str,
    range_start: int,
    range_end: int,
    range_step: int,
    base_inputs: dict | None,
) -> list[dict]:
    """Expand a range spec into a list of input dicts.

    Each dict is base_inputs | {range_param: index}.
    """
    base = base_inputs or {}
    return [{**base, range_param: i} for i in range(range_start, range_end, range_step)]


def cwl_to_jdl(
    task: CWLTask,
    job_hint: JobHint,
    matcher_docs: list[dict],
    input_params: dict | None,
) -> str:
    """Convert a CWL task with matcher docs into a JDL string.

    This is a transition-period function -- once JDL is retired,
    job attributes are populated directly from matcher docs.
    """
    jdl_fields: dict[str, Any] = {
        "Executable": "dirac-cwl-exec",
        "JobType": job_hint.type,
        "Priority": job_hint.priority,
        "LogLevel": job_hint.log_level,
    }

    # Derive JobName from CWL label/id
    task_label = getattr(task, "label", None)
    task_id = getattr(task, "id", None)
    if task_label:
        job_name = task_label
    elif task_id and task_id != ".":
        job_name = task_id.split("#")[-1].split("/")[-1]
    else:
        job_name = "cwl-job"

    # Append parametric key=value pairs to job name for disambiguation
    if input_params:
        scalars = [
            f"{k}={v}"
            for k, v in input_params.items()
            if isinstance(v, (int, float, str))
        ]
        if scalars:
            job_name = f"{job_name} ({', '.join(scalars)})"

    jdl_fields["JobName"] = job_name

    if job_hint.group:
        jdl_fields["JobGroup"] = job_hint.group

    # Extract fields from matcher docs for JDL (best-effort, lossy)
    if matcher_docs:
        first_doc = matcher_docs[0]

        if "site" in first_doc:
            # Collect all unique sites across all matcher docs
            sites = list({d["site"] for d in matcher_docs if "site" in d})
            jdl_fields["Site"] = sites

        if "cpu-work" in first_doc:
            jdl_fields["CPUTime"] = first_doc["cpu-work"]

        if "wall-time" in first_doc:
            jdl_fields["MaxWallTime"] = first_doc["wall-time"]

        cpu = first_doc.get("cpu", {})
        if isinstance(cpu, dict) and "num-cores" in cpu:
            cores = cpu["num-cores"]
            if isinstance(cores, dict):
                if "min" in cores and cores["min"] is not None:
                    jdl_fields["MinNumberOfProcessors"] = cores["min"]
                if "max" in cores and cores["max"] is not None:
                    jdl_fields["MaxNumberOfProcessors"] = cores["max"]
            else:
                jdl_fields["MinNumberOfProcessors"] = cores
                jdl_fields["MaxNumberOfProcessors"] = cores

        gpu = first_doc.get("gpu", {})
        if gpu:
            tags = {"GPU"}
            jdl_fields["Tags"] = list(set(jdl_fields.get("Tags", [])) | tags)

    # Auto-derive processor tags from JDL fields
    tags = set(jdl_fields.get("Tags", []))
    min_proc = jdl_fields.get("MinNumberOfProcessors", 1)
    max_proc = jdl_fields.get("MaxNumberOfProcessors")
    if min_proc and min_proc > 1:
        tags.add("MultiProcessor")
    if min_proc and max_proc and min_proc == max_proc:
        tags.add(f"{min_proc}Processors")
    if tags:
        jdl_fields["Tags"] = list(tags)

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
                if isinstance(val, dict) and (file_ref := _get_file_ref(val)):
                    sandbox_files.append(file_ref.split("#")[0])
                elif isinstance(val, list):
                    sandbox_files.extend(
                        file_ref.split("#")[0]
                        for item in val
                        if isinstance(item, dict) and (file_ref := _get_file_ref(item))
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
                if isinstance(val, dict) and (file_ref := _get_file_ref(val)):
                    lfns.append(file_ref)
                elif isinstance(val, list):
                    lfns.extend(
                        file_ref
                        for item in val
                        if isinstance(item, dict) and (file_ref := _get_file_ref(item))
                    )
        if lfns:
            jdl_fields["InputData"] = lfns

    # OutputSandbox
    sandbox_outputs: list[str] = []
    if hasattr(task, "stdout") and task.stdout:
        sandbox_outputs.append(task.stdout)
    if hasattr(task, "stderr") and task.stderr:
        sandbox_outputs.append(task.stderr)

    if job_hint.output_sandbox:
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

    # Merge legacy_jdl last (user overrides everything)
    jdl_fields.update(job_hint.legacy_jdl)

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
    """Submit CWL jobs: validate, build matcher docs, store workflow, create jobs."""
    workflow_id = compute_workflow_id(cwl_yaml)

    # INSERT IF NOT EXISTS — idempotent, content-addressed
    await job_db.insert_workflow(workflow_id, cwl_yaml, persistent=False)

    task = parse_cwl(cwl_yaml)

    # Validate all CWL requirements against the whitelist
    validate_requirements(task)

    job_hint = extract_job_hint(task)

    # Build matcher docs from hint + supported CWL requirements
    matcher_docs = build_matcher_docs(task, job_hint)

    inserted: list[InsertedJob] = []
    for input_params in input_yamls:
        # Validate File references (LFN:/SB: must be in location, not path)
        _validate_file_inputs(input_params)

        # Generate JDL for transition period
        jdl = cwl_to_jdl(task, job_hint, matcher_docs, input_params)

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
