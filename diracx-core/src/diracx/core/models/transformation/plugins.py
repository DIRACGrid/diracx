"""Contracts of the Transformation System plugins (DX-ADR-006).

Feeders and packers are plain async generator functions discovered through
entry points. Each kind fixes a set of keyword parameters and takes its
arguments as a ``PluginArgs`` model read from a JSON column. The ``Feeder`` and
``Packer`` Protocols write those signatures down so that mypy can check a
plugin against them: a plugin does not inherit from them, it only has to match.

A plugin may declare further keyword parameters for what the installation
provides (settings, databases). Each takes ``INJECTED`` as its default, which
keeps the plugin a match for its Protocol, and the core replaces it with an
instance of the parameter's annotated type when it calls the plugin.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any, Protocol, TypeVar

from pydantic import ConfigDict, JsonValue

from .._base import BaseModel
from .enums import InputStatus
from .feeder import FeederProposal
from .packer import PackerInput, PackerProposal


class _Injected:
    """Type of the ``INJECTED`` marker."""

    def __repr__(self) -> str:
        return "INJECTED"


INJECTED: Any = _Injected()
"""Default marking a plugin parameter the core supplies from its annotation.

Typed ``Any`` so it is accepted as the default whatever the parameter's type.
"""


class PluginArgs(BaseModel):
    """Base class of every plugin's arguments model.

    Arguments come from a JSON column written at submission and rewritten by
    hooks, so an unknown key is an error rather than silently ignored: a typo
    must not fall back to a default. Instances are immutable so a plugin cannot
    alter the arguments it was given.
    """

    model_config = ConfigDict(
        use_attribute_docstrings=True, extra="forbid", frozen=True
    )


# Contravariant: a plugin accepting any ``PluginArgs`` can stand in for one
# that expects a specific subclass, not the other way round.
ArgsT_contra = TypeVar("ArgsT_contra", bound=PluginArgs, contravariant=True)


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


class Packer(Protocol[ArgsT_contra]):
    """Signature every packer must have (DX-ADR-006)."""

    def __call__(
        self,
        *,
        packer_args: ArgsT_contra,
        inputs: AsyncIterator[PackerInput],
        flush: bool,
        feeder_active: bool,
        input_overview: Mapping[InputStatus, int],
    ) -> AsyncIterator[PackerProposal]:
        """Yield parcels, rejected inputs and delayed inputs.

        Args:
            packer_args: The transformation's validated ``PackerArgs``.
            inputs: The claimed ``Unassigned`` inputs.
            flush: Whether to pack groups that would otherwise be held back.
            feeder_active: Whether more inputs may still arrive.
            input_overview: Number of the transformation's inputs per status;
                read-only.
        """
        ...
