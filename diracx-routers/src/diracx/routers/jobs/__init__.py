from __future__ import annotations

__all__ = [
    "ActionType",
    "SandboxAccessPolicy",
    "WMSAccessPolicy",
    "EXAMPLE_SUMMARY",
    "EXAMPLE_HEARTBEAT",
    "EXAMPLE_METADATA",
    "EXAMPLE_STATUS_UPDATES",
    "router",
]

import logging

from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, SandboxAccessPolicy, WMSAccessPolicy
from .query import EXAMPLE_SUMMARY
from .query import router as query_router
from .sandboxes import router as sandboxes_router
from .status import EXAMPLE_HEARTBEAT, EXAMPLE_METADATA, EXAMPLE_STATUS_UPDATES
from .status import router as status_router
from .submission import router as submission_router

logger = logging.getLogger(__name__)

router = DiracxRouter()
router.include_router(sandboxes_router)
router.include_router(status_router)
router.include_router(query_router)
router.include_router(submission_router)
