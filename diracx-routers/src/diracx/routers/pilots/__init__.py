from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .credentials import router as credentials_router

logger = logging.getLogger(__name__)

router = DiracxRouter()
router.include_router(credentials_router)
