"""Feeder plugin proposals (DX-ADR-006).

Feeder plugins yield these values; only the core materialises them into rows.
Input identity hashing lives in ``diracx.core.hashing``.
"""

from __future__ import annotations

from typing import Any, Self

from pydantic import model_validator

from ._base import BaseModel
from .types import UTCDatetime

# JSON type alias for feeder bookmarks and descriptors (ADR-006).
JSONSerializable = dict[str, Any] | list[Any] | str | int | float | bool | None


class Input(BaseModel):
    """One input proposed by a feeder for insertion into ``Inputs``.

    May be a file (``lfn`` set), a seed/parameter set (descriptor only), or
    anything else the transformation consumes. The core fills identity, hash,
    and status when materialising the row.
    """

    descriptor: dict[str, JSONSerializable] | None = None
    """Opaque JSON describing the input; hashed with the LFN for deduplication."""
    lfn: str | None = None
    """Logical file name, or ``None`` for non-file inputs."""
    lfn_size: int | None = None
    """Size in bytes of the whole file, whatever a mask in ``descriptor`` selects."""
    delayed_until: UTCDatetime | None = None
    """If set, the packer must not claim this input before this UTC time."""

    @model_validator(mode="after")
    def _lfn_size_requires_lfn(self) -> Self:
        """Reject a size without an LFN."""
        if self.lfn_size is not None and self.lfn is None:
            raise ValueError("lfn_size requires lfn")
        return self


class SetFeederState(BaseModel):
    """Bookmark update; commits with inputs yielded before it in the same batch."""

    state: dict[str, JSONSerializable]
    """Opaque feeder bookmark stored in ``Transformations.FeederState``."""


class NoMoreInputs(BaseModel):
    """Signal that the feeder will produce no further inputs.

    The core clears ``Transformations.FeederEnabled`` when this is yielded.
    """
