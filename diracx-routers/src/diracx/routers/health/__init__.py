from __future__ import annotations

__all__ = ["router"]

from ..fastapi_classes import DiracxRouter
from .probes import router as probes_router

router = DiracxRouter(require_auth=False, include_in_schema=False)
router.include_router(probes_router)
