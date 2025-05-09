from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .jobs import router as jobs_router

logger = logging.getLogger(__name__)

router = DiracxRouter()
router.include_router(jobs_router)
