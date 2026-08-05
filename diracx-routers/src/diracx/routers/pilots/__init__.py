from __future__ import annotations

__all__ = [
    "ActionType",
    "PilotManagementAccessPolicy",
    "router",
]

import logging

from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, PilotManagementAccessPolicy
from .management import router as management_router
from .query import router as query_router

logger = logging.getLogger(__name__)

router = DiracxRouter()
router.include_router(management_router)
router.include_router(query_router)
