from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .query import router as query_router
from .sandboxes import router as sandboxes_router
from .status import router as status_router
from .submission import router as submission_router

logger = logging.getLogger(__name__)

router = DiracxRouter()
router.include_router(sandboxes_router)
router.include_router(status_router)
router.include_router(query_router)
router.include_router(submission_router)
