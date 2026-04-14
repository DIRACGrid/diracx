"""Models for CWL workflow submission via the dirac:Job hint."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class IOSource(BaseModel):
    """Reference to a CWL input or output by its ID."""

    source: str


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

    Matching is expressed via a list of matching specification documents
    in the ``matcher`` field. Each document describes an environment the
    job can run in (OR semantics across documents).
    """

    schema_version: Literal["1.0"] = "1.0"

    # Matching specification documents (OR semantics across docs)
    matcher: list[Any] = []

    # JDL escape hatch (transition period)
    legacy_jdl: dict[str, Any] = {}

    # Job metadata
    type: str = "User"
    group: str = ""
    log_level: str = "INFO"
    priority: int = 5

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
