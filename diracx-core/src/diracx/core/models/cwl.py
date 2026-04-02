"""Models for CWL workflow submission via the dirac:Job hint."""

from __future__ import annotations

from pydantic import BaseModel


class IOSource(BaseModel):
    """Reference to a CWL input or output by its ID."""

    source: str
    path: str | None = (
        None  # relative path within job working directory (input_sandbox only)
    )


class OutputDataEntry(BaseModel):
    """Output data entry with per-output SE and LFN path."""

    source: str
    output_path: str
    output_se: list[str] = ["SE-USER"]


class JobHint(BaseModel):
    """Unified DIRAC-specific hint for job scheduling and I/O.

    Resource requirements (cores, RAM) are expressed via standard CWL
    requirements, not in this hint.

    Execution hooks are determined automatically by ``type``, not
    configured by the submitter.

    I/O fields reference CWL input/output IDs via ``source:`` syntax,
    consistent with CWL's own referencing conventions.
    """

    schema_version: str  # required, e.g. "1.0"

    # Scheduling (DIRAC-specific, no CWL equivalent)
    priority: int = 5
    cpu_work: int | None = None  # HS06-seconds → JDL CPUTime
    platform: str | None = None
    sites: list[str] | None = None
    banned_sites: list[str] | None = None
    tags: list[str] | None = None  # merged with auto-derived tags

    # Job metadata
    type: str = "User"
    group: str = ""
    log_level: str = "INFO"

    # I/O: reference CWL input/output IDs via source:
    input_sandbox: list[IOSource] = []
    input_data: list[IOSource] = []
    output_sandbox: list[IOSource] = []
    output_data: list[OutputDataEntry] = []

    @classmethod
    def from_cwl(cls, cwl_object) -> JobHint:
        """Extract a JobHint from a CWL object's hints list."""
        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if isinstance(hint, dict) and hint.get("class") == "dirac:Job":
                data = {k: v for k, v in hint.items() if k != "class"}
                return cls(**data)
        raise ValueError("CWL task is missing required dirac:Job hint")
