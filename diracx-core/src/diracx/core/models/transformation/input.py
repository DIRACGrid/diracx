"""Input fields shared by feeder and packer plugins."""

from __future__ import annotations

import json

from pydantic import Field, JsonValue, field_validator

from .._base import BaseModel


class TransformationInput(BaseModel, frozen=True, allow_inf_nan=False):
    """Describe a file or parameter set consumed by a transformation."""

    descriptor: dict[str, JsonValue] = {}
    """Opaque JSON describing the input."""
    lfn: str | None = Field(default=None, min_length=1, max_length=512)
    """Logical file name, or ``None`` for non-file inputs."""
    lfn_size: int | None = Field(default=None, ge=0)
    """Size in bytes of the whole file, whatever the descriptor selects."""

    @field_validator("descriptor")
    @classmethod
    def _descriptor_has_finite_numbers(
        cls, value: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        """Check JSON-parsed values that bypass JsonValue's Python validation."""
        json.dumps(value, allow_nan=False)
        return value
