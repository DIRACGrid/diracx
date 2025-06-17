from __future__ import annotations

import logging

from ..fastapi_classes import DiracxRouter
from .management import router as management_router
from .query import router as query_router
from .resources import router as resources_router

logger = logging.getLogger(__name__)

router = DiracxRouter(require_auth=True)
router.include_router(management_router)
router.include_router(query_router)
<<<<<<< HEAD
=======
router.include_router(auth_router)
router.include_router(resources_router)
>>>>>>> f9ce5a1 (feat: Add pilot logging)
