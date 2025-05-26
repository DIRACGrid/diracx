from __future__ import annotations

from ..fastapi_classes import DiracxRouter
from .auth import router as auth_router
from .logging import router as logging_router

# TODO: Change this ugly require_auth
router = DiracxRouter(require_auth=False)
router.include_router(auth_router)
router.include_router(logging_router)
