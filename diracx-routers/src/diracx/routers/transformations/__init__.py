"""Routers of the Transformation System (DX-ADR-002 to DX-ADR-009)."""

from __future__ import annotations

__all__ = ["TransformationAccessPolicy", "router"]

from ..fastapi_classes import DiracxRouter
from .access_policies import TransformationAccessPolicy
from .workgraphs import router as workgraphs_router

router = DiracxRouter()
router.include_router(workgraphs_router)
