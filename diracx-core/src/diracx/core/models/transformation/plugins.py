"""Shared pieces of the Transformation System plugin contracts (DX-ADR-006).

Feeders and packers are plain async generator functions discovered through
entry points. Their contracts, the ``Feeder`` and ``Packer`` Protocols, live
next to the proposals they yield in ``feeder.py`` and ``packer.py``; mypy checks
a plugin against them by signature, a plugin never inherits from them. This
module holds what both share: the base of every arguments model, and the
``INJECTED`` marker for the parameters the core supplies from the installation.
"""

from __future__ import annotations

from typing import Any, TypeVar

from pydantic import ConfigDict

from .._base import BaseModel


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

    The plugin defines the fields; each transformation stores its own values,
    set by the workgraph's submitter and possibly changed later by hooks. An
    unknown key is an error rather than ignored, so a typo cannot silently fall
    back to a default, and instances are immutable so a plugin cannot alter the
    arguments it was given.
    """

    model_config = ConfigDict(
        use_attribute_docstrings=True, extra="forbid", frozen=True
    )


# Contravariant because plugins receive their arguments: a plugin accepting any
# PluginArgs can stand in for one expecting a subclass. mypy requires it for a
# type variable used only in a Protocol's parameters.
ArgsT_contra = TypeVar("ArgsT_contra", bound=PluginArgs, contravariant=True)
