from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Callable, TypeVar, cast

from fastapi import APIRouter, FastAPI
from starlette.routing import Route

T = TypeVar("T")


def _downgrade_openapi_schema(data):
    """Modify an openapi schema in-place to be compatible with AutoRest."""
    if isinstance(data, dict):
        for k, v in list(data.items()):
            if k == "anyOf":
                if {"type": "null"} in v:
                    v.pop(v.index({"type": "null"}))
                    data["nullable"] = True
                    if len(v) == 1:
                        data |= v[0]
            elif k == "const":
                data.pop(k)
            # https://github.com/fastapi/fastapi/discussions/12984
            elif k == "propertyNames":
                data.pop(k)

            _downgrade_openapi_schema(v)
    if isinstance(data, list):
        for v in data:
            _downgrade_openapi_schema(v)


class DiracFastAPI(FastAPI):
    def __init__(self):
        @contextlib.asynccontextmanager
        async def lifespan(app: DiracFastAPI):
            async with contextlib.AsyncExitStack() as stack:
                await asyncio.gather(
                    *(stack.enter_async_context(f()) for f in app.lifetime_functions)
                )
                yield

        self.lifetime_functions = []
        super().__init__(
            swagger_ui_init_oauth={
                "clientId": "myDIRACClientID",
                "scopes": "property:NormalUser",
                "usePkceWithAuthorizationCodeGrant": True,
            },
            generate_unique_id_function=lambda route: f"{route.tags[0]}_{route.name}",
            title="Dirac",
            lifespan=lifespan,
            openapi_url="/api/openapi.json",
            docs_url="/api/docs",
            swagger_ui_oauth2_redirect_url="/api/docs/oauth2-redirect",
        )
        # FIXME: when autorest will support 3.1.0
        # From 0.99.0, FastAPI is using openapi 3.1.0 by default
        # This version is not supported by autorest yet
        self.openapi_version = "3.0.2"

    def openapi(self, *args, **kwargs):
        if not self.openapi_schema:
            super().openapi(*args, **kwargs)
            _downgrade_openapi_schema(self.openapi_schema)

            # Remove 422 responses as we don't want autorest to use it
            for _, method_item in self.openapi_schema.get("paths").items():
                for _, param in method_item.items():
                    responses = param.get("responses")
                    if "422" in responses:
                        del responses["422"]

        return self.openapi_schema


class DiracxRouter(APIRouter):
    def __init__(
        self,
        *,
        dependencies=None,
        require_auth: bool = True,
        include_in_schema: bool = True,
        path_root: str = "/api",
    ):
        super().__init__(dependencies=dependencies, include_in_schema=include_in_schema)
        self.diracx_require_auth = require_auth
        self.diracx_path_root = path_root

    ####
    # These 2 methods are needed to overwrite routes
    # https://github.com/tiangolo/fastapi/discussions/8489

    def add_api_route(self, path: str, endpoint: Callable[..., Any], **kwargs):

        route_index = self._get_route_index_by_path_and_methods(
            path, set(kwargs.get("methods", []))
        )
        if route_index >= 0:
            self.routes.pop(route_index)

        return super().add_api_route(path, endpoint, **kwargs)

    def _get_route_index_by_path_and_methods(self, path: str, methods: set[str]) -> int:
        routes = cast(list[Route], self.routes)
        for index, route in enumerate(routes):
            if route.path == path and methods == route.methods:
                return index
        return -1

    ######
