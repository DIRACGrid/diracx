from __future__ import annotations

from diracx.core.models import Metadata

__all__ = ["ExtendedMetadata"]


class ExtendedMetadata(Metadata):
    gubbins_secrets: str
    gubbins_user_info: dict[str, list[str | None]]
