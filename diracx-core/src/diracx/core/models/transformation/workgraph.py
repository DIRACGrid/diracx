"""Models of workgraph submission (DX-ADR-004, DX-ADR-007).

The ``dirac:`` hint vocabulary a workgraph document carries, transcribed from
the provisional ``docs/schemas/dirac-1.0.json``, and the response of a
submission. Field lengths mirror the columns they are compiled into, so an
over-long value is rejected at validation rather than by the database.

Only the hints are modelled here: the CWL document itself is plain JSON, and
its validation as CWL (``cwl-utils``) belongs to process ingestion
(DIRACGrid/diracx#1080).
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "ActionSpec",
    "DataManagementHint",
    "FeederHint",
    "PackerSpec",
    "SubmittedTransformation",
    "TargetSpec",
    "TransformationHint",
    "WorkgraphHint",
    "WorkgraphSubmissionResult",
]

# Names stored in VARCHAR(64) columns: Packer, Feeder, Action, Output names
Name64 = Field(min_length=1, max_length=64)

InitialState = Literal["Active", "Stopped", "Paused"]


class _Hint(BaseModel):
    """A hint object: unknown fields are an error, as in the JSON schema."""

    model_config = ConfigDict(extra="forbid")


class ActionSpec(_Hint):
    """One entry of an ordered lifecycle-action list (DX-ADR-006)."""

    action: str = Name64
    args: dict[str, Any] | None = None


class PackerSpec(_Hint):
    """How inputs are grouped into parcels: a plugin and its arguments."""

    name: str = Name64
    args: dict[str, Any] | None = None


class FeederHint(_Hint):
    """``dirac:Feeder``, on a workflow input parameter."""

    name: str = Name64
    args: dict[str, Any] | None = None


class TargetSpec(_Hint):
    """What the workgraph is for: a workflow output and its size."""

    output: str
    files: int | None = Field(default=None, ge=1)


class TransformationHint(_Hint):
    """``dirac:Transformation``, on a step: the transformation it becomes."""

    packer: PackerSpec | None = None
    driving_input: str | None = None
    after: list[str] = Field(default_factory=list)
    initial_state: InitialState | None = None
    hooks: dict[str, Any] | None = None
    actions: dict[str, list[ActionSpec]] = Field(default_factory=dict)
    output_sandbox: list[str] = Field(default_factory=list)
    requirements_template: dict[str, Any] | None = None


class DataManagementHint(_Hint):
    """A replication, removal or archival declared in ``dirac:Workgraph``."""

    id: str | None = Field(default=None, min_length=1, max_length=64)
    operation: Literal["replicate", "remove", "archive"]
    files: str
    destination: str | list[str] | None = None
    storage: str | list[str] | None = None
    after: list[str] = Field(default_factory=list)
    initial_state: InitialState | None = None
    packer: PackerSpec | None = None
    actions: dict[str, list[ActionSpec]] = Field(default_factory=dict)
    output_sandbox: list[str] = Field(default_factory=list)


class WorkgraphHint(_Hint):
    """``dirac:Workgraph``, on the top-level ``Workflow``."""

    schema_version: Literal["1.0"]
    type: str | None = Field(default=None, min_length=1, max_length=32)
    target: TargetSpec | None = None
    output_sandbox: list[str] = Field(default_factory=list)
    hooks: dict[str, Any] | None = None
    approving: list[ActionSpec] = Field(default_factory=list)
    data_management: list[DataManagementHint] = Field(default_factory=list)


class SubmittedTransformation(BaseModel):
    """A transformation created by a workgraph submission."""

    transformation_id: UUID
    name: str
    kind: Literal["Compute", "Data"]


class WorkgraphSubmissionResult(BaseModel):
    """What a successful workgraph submission created."""

    workgraph_id: UUID
    transformations: list[SubmittedTransformation]
