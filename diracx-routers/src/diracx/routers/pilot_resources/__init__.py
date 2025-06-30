from __future__ import annotations

import logging

from fastapi import Depends

from diracx.routers.utils.pilots import (
    verify_dirac_pilot_access_token,
)

from ..fastapi_classes import DiracxRouter
from .util import router as util_router

logger = logging.getLogger(__name__)


# Require_auth set to False because it adds *user* auth, and not pilot's
# So we add it manually
router = DiracxRouter(
    require_auth=False, dependencies=[Depends(verify_dirac_pilot_access_token)]
)

router.include_router(util_router)
