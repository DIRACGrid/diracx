from __future__ import annotations

from logging import getLogger

from ..fastapi_classes import DiracxRouter
from .logging import router as logging_router

logger = getLogger(__name__)

router = DiracxRouter()
router.include_router(logging_router)
