from __future__ import annotations

__all__ = ["DiracxRouter"]

import asyncio
import contextlib
from typing import Any, Callable, TypeVar

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute

from diracx.tasks.plumbing.depends import auto_inject

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


def _pop_overridden_route(router: APIRouter, path: str, methods: set[str]) -> bool:
    """Remove the first route matching ``path`` and ``methods`` from a router.

    The route is searched in the router itself and, since FastAPI 0.137
    where ``include_router`` preserves the original routers instead of
    cloning their routes, in the routers it includes.

    Returns:
        True if a route was removed.
    """
    for index, route in enumerate(router.routes):
        if isinstance(route, APIRoute):
            if route.path == path and methods == route.methods:
                router.routes.pop(index)
                return True
        else:
            included_router = getattr(route, "original_router", None)
            if isinstance(included_router, APIRouter) and _pop_overridden_route(
                included_router, path, methods
            ):
                return True
    return False


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
        endpoint = auto_inject(endpoint)

        _pop_overridden_route(self, path, set(kwargs.get("methods", [])))

        return super().add_api_route(path, endpoint, **kwargs)

    ######
