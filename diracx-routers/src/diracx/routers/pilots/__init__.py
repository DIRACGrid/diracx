from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .auth import router as auth_router
from .management import router as management_router
from .query import router as query_router

logger = logging.getLogger(__name__)

router = DiracxRouter(require_auth=False)
router.include_router(management_router)
router.include_router(query_router)
router.include_router(auth_router)
