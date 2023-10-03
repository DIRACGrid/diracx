from __future__ import annotations

from pydantic import BaseModel, root_validator
from datetime import datetime, timezone
from typing import Annotated, Any, TypedDict

from fastapi import (
    Body,
    Depends,
    Header,
    HTTPException,
    Response,
    status,
)

from diracx.core.config import Config, ConfigSource
from diracx.routers.auth import UserInfo, has_properties, verify_dirac_token

from diracx.routers.fastapi_classes import DiracxRouter
from mydiracx.db.mydb.db import myDB

LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"

router = DiracxRouter()
#router = DiracxRouter(require_auth=False) #also comment user_info for local tests

class CustomObject(TypedDict):
    PathValueAsString: str
    IntegerValue: int

EXAMPLE_CUSTOMOBJECT = {
 "PathValueAsString": "/example/test",
 "IntegerValue": 1,
}

EXAMPLE_PATHVALUE = "/example/test"

@router.get("/example/search/{path_value:path}")
async def get_example_object(
    user_info: Annotated[UserInfo, Depends(verify_dirac_token)],
    my_db: Annotated[myDB, Depends(myDB.transaction)],
    #path_value: Annotated[str, Body(examples=EXAMPLE_PATHVALUE)],
    path_value: str,
):
    return await my_db.search(path_value)

@router.post("/example/post")
async def post_object(
    user_info: Annotated[UserInfo, Depends(verify_dirac_token)],
    my_db: Annotated[myDB, Depends(myDB.transaction)],
    body: Annotated[CustomObject, Body(examples=EXAMPLE_CUSTOMOBJECT)],

):
    return await my_db.insert(body["PathValueAsString"],body["IntegerValue"])
