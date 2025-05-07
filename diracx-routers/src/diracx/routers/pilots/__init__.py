from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .management import router as management_router

logger = logging.getLogger(__name__)

router = DiracxRouter()
router.include_router(management_router)
