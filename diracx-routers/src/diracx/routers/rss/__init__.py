from __future__ import annotations

from ..fastapi_classes import DiracxRouter
from .rss import router as rss_router

router = DiracxRouter()
router.include_router(rss_router)
