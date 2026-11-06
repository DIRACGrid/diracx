"""The packer contract of the Transformation System (DX-ADR-006).

A packer plugin is offered ``PackerInput``s and yields proposals: the parcels
to make, and the inputs to reject or hold back. The core checks every
proposal and is the only thing that writes.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from ..types import UTCDatetime


class PackerInput(BaseModel):
    """An ``Unassigned`` input claimed by the core and offered to a packer.

    Attributes:
        input_id: Identifier of the input.
        parent_input_id: Input this one was split from, if any.
        lfn: Logical file name, if the input is a file.
        lfn_size: Size of the whole file, whatever the descriptor selects.
        descriptor: Everything else describing the input.
        error_count: Failed parcels the input was part of since the last reset.
    """

    model_config = ConfigDict(frozen=True)

    input_id: UUID
    parent_input_id: UUID | None = None
    lfn: str | None = Field(default=None, min_length=1, max_length=512)
    lfn_size: int | None = Field(default=None, ge=0)
    descriptor: dict[str, JsonValue] = {}
    error_count: int = Field(default=0, ge=0)


class ComputeParcel(BaseModel):
    """A packer's proposal for a parcel that runs as a job.

    Attributes:
        input_ids: Inputs the parcel is made of.
        metadata: Free-form annotations stored on the parcel.
        requirements: Additions merged recursively over the transformation's
            requirements template: nested mappings are merged key by key, and
            any other value, lists included, replaces the template's.
        parameters: The CWL input object passed to the runner.
    """

    input_ids: list[UUID] = Field(min_length=1)
    metadata: dict[str, JsonValue] = {}
    requirements: dict[str, JsonValue] | None = None
    parameters: dict[str, JsonValue] = {}


class StorageDelta(BaseModel):
    """What a data parcel's request costs one storage element.

    Attributes:
        se: Storage element affected.
        files: Files added there if positive, freed if negative.
        bytes: Bytes added there if positive, freed if negative.
    """

    se: str
    files: int
    bytes: int


class DataParcel(BaseModel):
    """A packer's proposal for a parcel that runs as a request.

    Attributes:
        input_ids: Inputs the parcel is made of.
        metadata: Free-form annotations stored on the parcel.
        request: DIRAC RMS request body (DX-ADR-003).
        deltas: What the request costs each storage element.
    """

    input_ids: list[UUID] = Field(min_length=1)
    metadata: dict[str, JsonValue] = {}
    request: dict[str, JsonValue]
    deltas: list[StorageDelta] = []


class BadInput(BaseModel):
    """A packer's verdict that an input cannot be used at all.

    The core moves the input to ``Problematic`` with the reason.

    Attributes:
        input_id: The input rejected.
        reason: Why the packer rejected it.
    """

    input_id: UUID
    reason: str = "Unknown reason"


class DelayInput(BaseModel):
    """A packer's request to withhold inputs from packing for a while.

    Attributes:
        input_ids: The inputs to withhold.
        delay_until: Time before which they are not offered again.
    """

    input_ids: list[UUID] = Field(min_length=1)
    delay_until: UTCDatetime


PackerProposal = ComputeParcel | DataParcel | BadInput | DelayInput
