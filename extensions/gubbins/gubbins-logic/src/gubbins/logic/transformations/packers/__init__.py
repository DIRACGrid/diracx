"""Packer plugins shipped with gubbins (DX-ADR-006)."""

from __future__ import annotations

__all__ = [
    "TrivialPackerArgs",
    "trivial_packer",
]

from .trivial import TrivialPackerArgs, trivial_packer
