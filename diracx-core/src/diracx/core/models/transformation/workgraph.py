"""Models of workgraph submission (DX-ADR-004, DX-ADR-007).

The subset of the ``dirac:`` hint vocabulary (``docs/schemas/dirac-1.0.json``)
that a workgraph made of a single compute transformation needs, and the
response of a submission. Hint fields beyond this subset are rejected rather
than ignored, so a document never silently loses what it asked for. Field
lengths mirror the columns they are compiled into.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Hint(BaseModel):
    """A hint object: unknown fields are an error."""

    model_config = ConfigDict(extra="forbid")


class PackerSpec(_Hint):
    """How inputs are grouped into parcels: a plugin and its arguments."""

    name: str = Field(min_length=1, max_length=64)
    args: dict[str, Any] | None = None


class FeederHint(_Hint):
    """``dirac:Feeder``, on a workflow input parameter."""

    name: str = Field(min_length=1, max_length=64)
    args: dict[str, Any] | None = None


class TransformationHint(_Hint):
    """``dirac:Transformation``, on a step: the transformation it becomes."""

    # Required until defaults are resolved from the configuration service
    packer: PackerSpec
    requirements_template: dict[str, Any] | None = None


class WorkgraphHint(_Hint):
    """``dirac:Workgraph``, on the top-level ``Workflow``."""

    schema_version: Literal["1.0"]
    # Required until defaults are resolved from the configuration service
    type: str = Field(min_length=1, max_length=32)


class WorkgraphSubmissionResult(BaseModel):
    """What a successful workgraph submission created."""

    workgraph_id: UUID
    transformation_id: UUID
