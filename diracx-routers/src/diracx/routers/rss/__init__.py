from __future__ import annotations

__all__ = ["RSSAccessPolicy", "router"]

from ..fastapi_classes import DiracxRouter
from .access_policies import RSSAccessPolicy
from .rss import router as rss_router

router = DiracxRouter()
router.include_router(rss_router)
