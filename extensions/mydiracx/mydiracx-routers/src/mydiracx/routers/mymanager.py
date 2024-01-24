from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Literal, TypedDict
from uuid import UUID, uuid4

import httpx
from authlib.integrations.starlette_client import OAuthError
from authlib.jose import JoseError, JsonWebKey, JsonWebToken
from authlib.oidc.core import IDToken
from cachetools import TTLCache
from fastapi import (
    Depends,
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    responses,
    status,
)
from fastapi.responses import RedirectResponse
from fastapi.security import OpenIdConnect
from pydantic import BaseModel, Field

from diracx.core.exceptions import (
    DiracHttpResponse,
    ExpiredFlowError,
    PendingAuthorizationError,
)
from diracx.core.models import TokenResponse, UserInfo
from diracx.core.properties import (
    PROXY_MANAGEMENT,
    SecurityProperty,
    UnevaluatedProperty,
)
from diracx.core.settings import ServiceSettingsBase, TokenSigningKey
from diracx.db.sql.auth.schema import FlowStatus, RefreshTokenStatus

from .dependencies import (
    DummyDB,
)
from diracx.routers.fastapi_classes import DiracxRouter

oidc_scheme = OpenIdConnect(openIdConnectUrl="/.well-known/openid-configuration")


router = DiracxRouter(require_auth=False)

@router.get("/example/get_summary")
async def get_example_object(
    #user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dummy_db: DummyDB,
):

    #return await dummy_db.summary()
    return
