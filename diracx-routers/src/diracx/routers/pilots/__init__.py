from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .auth import router as auth_router
from .jobs import router as jobs_router

logger = logging.getLogger(__name__)

# TODO: Change this ugly require_auth
router = DiracxRouter(require_auth=False)
router.include_router(auth_router)
router.include_router(jobs_router)
