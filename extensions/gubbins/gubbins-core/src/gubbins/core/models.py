"""Gubbins-specific models."""

from __future__ import annotations

from diracx.core.models import Metadata

__all__ = ["ExtendedMetadata"]


class ExtendedMetadata(Metadata):
    """DiracX metadata extended with Gubbins-specific fields."""

    gubbins_secrets: str
    """Secrets specific to Gubbins."""

    gubbins_user_info: dict[str, list[str | None]]
    """Gubbins-specific information about the users, keyed by VO."""
