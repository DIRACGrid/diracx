"""The feeder contract of the Transformation System (DX-ADR-006).

Feeder plugins yield proposals: inputs to insert, bookmark updates, and an
exhaustion signal. The core materialises them into rows and is the only thing
that writes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Protocol, Self

from pydantic import Field, JsonValue, model_validator

from .._base import BaseModel
from ..types import UTCDatetime
from .plugins import ArgsT_contra


class FeederInput(BaseModel):
    """One input proposed by a feeder for insertion into ``Inputs``.

    May be a file (``lfn`` and ``lfn_size`` both set), a seed/parameter set
    (descriptor only), or anything else the transformation consumes. The core
    fills identity and status when materialising the row.
    """

    descriptor: dict[str, JsonValue] = {}
    """Opaque JSON describing the input."""
    lfn: str | None = Field(default=None, min_length=1, max_length=512)
    """Logical file name, or ``None`` for non-file inputs."""
    lfn_size: int | None = Field(default=None, ge=0)
    """Size in bytes of the whole file; required together with ``lfn``."""
    delayed_until: UTCDatetime | None = None
    """If set, the packer must not claim this input before this UTC time."""

    @model_validator(mode="after")
    def _lfn_and_lfn_size_together(self) -> Self:
        """Require ``lfn`` and ``lfn_size`` both set or both omitted."""
        if (self.lfn is None) != (self.lfn_size is None):
            raise ValueError("lfn and lfn_size must both be set or both be omitted")
        return self


class SetFeederState(BaseModel):
    """Bookmark update; commits with inputs yielded before it in the same batch."""

    state: dict[str, JsonValue]
    """Opaque feeder bookmark stored in ``Transformations.FeederState``."""


class NoMoreFeederInputs(BaseModel):
    """Signal that the feeder will produce no further inputs.

    The core clears ``Transformations.FeederEnabled`` when this is yielded.
    """


FeederProposal = FeederInput | SetFeederState | NoMoreFeederInputs


class Feeder(Protocol[ArgsT_contra]):
    """Signature every feeder must have (DX-ADR-006).

    Written as a plain ``def`` returning an ``AsyncIterator`` because that is
    the type of an async generator function: calling it returns the iterator
    straight away, there is nothing to await.
    """

    def __call__(
        self,
        *,
        feeder_args: ArgsT_contra,
        feeder_state: Mapping[str, JsonValue],
        scouting: bool,
    ) -> AsyncIterator[FeederProposal]:
        """Yield inputs, bookmark updates and an exhaustion signal.

        Args:
            feeder_args: The transformation's validated ``FeederArgs``.
            feeder_state: The bookmark last stored with ``SetFeederState``;
                read-only, a new bookmark is yielded instead.
            scouting: Whether the workgraph is in its scouting phase.
        """
        ...
