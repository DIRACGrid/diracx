from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .logs import router as legacy_router

logger = logging.getLogger(__name__)

router = DiracxRouter(require_auth=False)
router.include_router(legacy_router)
