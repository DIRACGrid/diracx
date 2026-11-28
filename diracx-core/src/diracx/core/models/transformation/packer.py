"""The packer contract of the Transformation System (DX-ADR-006).

A packer plugin is offered ``PackerInput``s and yields proposals: the parcels
to make, and the inputs to reject or hold back. The core checks every
proposal and is the only thing that writes.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import Field, JsonValue

from .._base import BaseModel
from ..types import UTCDatetime
from .input import TransformationInput


class PackerInput(TransformationInput):
    """An ``Unassigned`` input claimed by the core and offered to a packer."""

    input_id: UUID
    """Identifier of the input."""
    parent_input_id: UUID | None = None
    """Input this one was split from, if any."""
    error_count: int = Field(default=0, ge=0)
    """Failed parcels the input was part of since the last reset."""


class ComputeParcel(BaseModel):
    """A packer's proposal for a parcel that runs as a job."""

    input_ids: list[UUID] = Field(min_length=1)
    """Inputs the parcel is made of."""
    metadata: dict[str, JsonValue] = {}
    """Free-form annotations stored on the parcel."""
    requirements: dict[str, JsonValue] | None = None
    """Additions merged recursively over the transformation's requirements template.

    Nested mappings are merged key by key, and any other value, lists included,
    replaces the template's.
    """
    parameters: dict[str, JsonValue] = {}
    """The CWL input object passed to the runner."""


class StorageDelta(BaseModel):
    """What a data parcel's request costs one storage element."""

    se: str
    """Storage element affected."""
    files: int
    """Files added there if positive, freed if negative."""
    bytes: int
    """Bytes added there if positive, freed if negative."""


class DataParcel(BaseModel):
    """A packer's proposal for a parcel that runs as a request."""

    input_ids: list[UUID] = Field(min_length=1)
    """Inputs the parcel is made of."""
    metadata: dict[str, JsonValue] = {}
    """Free-form annotations stored on the parcel."""
    request: dict[str, JsonValue]
    """DIRAC RMS request body (DX-ADR-003)."""
    deltas: list[StorageDelta] = []
    """What the request costs each storage element."""


class BadInput(BaseModel):
    """A packer's verdict that an input cannot be used at all.

    The core moves the input to ``Problematic`` with the reason.
    """

    input_id: UUID
    """The input rejected."""
    reason: str = "Unknown reason"
    """Why the packer rejected it."""


class DelayInput(BaseModel):
    """A packer's request to withhold inputs from packing for a while."""

    input_ids: list[UUID] = Field(min_length=1)
    """The inputs to withhold."""
    delay_until: UTCDatetime
    """Time before which they are not offered again."""


PackerProposal = ComputeParcel | DataParcel | BadInput | DelayInput
