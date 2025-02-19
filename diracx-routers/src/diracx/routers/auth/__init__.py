from __future__ import annotations

from ..fastapi_classes import DiracxRouter
from ..utils.users import verify_dirac_access_token
from .authorize_code_flow import router as authorize_code_flow_router
from .device_flow import router as device_flow_router
from .management import router as management_router
from .token import router as token_router
from .utils import has_properties

router = DiracxRouter(require_auth=False)
router.include_router(device_flow_router)
router.include_router(management_router)
router.include_router(authorize_code_flow_router)
router.include_router(token_router)

__all__ = ["has_properties", "verify_dirac_access_token"]
