"""Provide FastAPI application and router classes customised for DiracX.

The application manages dependency lifetimes and adapts its OpenAPI schema for
client generation. The router injects dependencies and permits route overrides.
"""

from __future__ import annotations

__all__ = ["DiracxRouter"]

import asyncio
import contextlib
import copy
import dataclasses
from typing import Any, Callable, TypeVar

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute, _IncludedRouter
from starlette.routing import BaseRoute

from diracx.tasks.plumbing.depends import auto_inject

T = TypeVar("T")


def _downgrade_openapi_schema(data):
    """Convert selected OpenAPI schema constructs to AutoRest-compatible form.

    The conversion is performed in place: nullable unions become
    ``nullable`` schemas, and unsupported ``const`` and ``propertyNames``
    entries are removed recursively.

    Args:
        data: OpenAPI schema value to process, typically a dictionary or list.
    """
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
    """FastAPI application with DiracX lifespan and OpenAPI configuration."""

    def __init__(self):
        """Initialize the application, Swagger OAuth settings, and lifespan.

        The lifespan enters each context manager registered in
        ``lifetime_functions`` and exits them when the application shuts down.
        """

        @contextlib.asynccontextmanager
        async def lifespan(app: DiracFastAPI):
            """Manage the lifetimes registered on the application.

            Args:
                app: Application whose lifetime context managers are entered.

            Yields:
                Control while the application is running.
            """
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
        """Generate and cache the AutoRest-compatible OpenAPI schema.

        The generated schema is downgraded for AutoRest compatibility and 422
        validation responses are removed before it is cached.

        Args:
            *args: Positional arguments forwarded to FastAPI's schema generator.
            **kwargs: Keyword arguments forwarded to FastAPI's schema generator.

        Returns:
            The cached OpenAPI schema, generating it on the first call.
        """
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


def _without_overridden_route(
    routes: list[BaseRoute], path: str, methods: set[str], prefix: str = ""
) -> list[BaseRoute] | None:
    """Return a copy of ``routes`` without the first route matching ``path`` and ``methods``.

    The route is searched in ``routes`` and, since FastAPI 0.137 where
    ``include_router`` preserves the original routers instead of cloning their
    routes, in the routers they include. An included router containing the route
    is replaced by a copy without it, as the original can be shared, e.g. an
    extension includes and overrides the module level routers of DiracX.

    Args:
        routes: Routes to search.
        path: Full path of the route, including the prefix of every include.
        methods: HTTP methods of the route.
        prefix: Prefix under which ``routes`` are served.

    Returns:
        The new list of routes, or None if no route matched.
    """
    for index, route in enumerate(routes):
        if isinstance(route, APIRoute):
            if prefix + route.path == path and methods == route.methods:
                return [*routes[:index], *routes[index + 1 :]]
        elif isinstance(route, _IncludedRouter):
            included_routes = _without_overridden_route(
                route.original_router.routes,
                path,
                methods,
                prefix + route.include_context.prefix,
            )
            if included_routes is not None:
                included_router = copy.copy(route.original_router)
                included_router.routes = included_routes
                included_router._low_priority_routes = list(
                    included_router._low_priority_routes
                )
                included_route = _IncludedRouter(
                    original_router=included_router,
                    include_context=dataclasses.replace(
                        route.include_context, included_router=included_router
                    ),
                )
                return [*routes[:index], included_route, *routes[index + 1 :]]
    return None


class DiracxRouter(APIRouter):
    """APIRouter carrying DiracX authentication and mount configuration."""

    def __init__(
        self,
        *,
        dependencies=None,
        require_auth: bool = True,
        include_in_schema: bool = True,
        path_root: str = "/api",
    ):
        """Initialize a router with DiracX-specific mounting options.

        Args:
            dependencies: Dependencies applied to routes registered on the router.
            require_auth: Whether the application factory should require authentication.
            include_in_schema: Whether the router's routes are included in OpenAPI.
            path_root: Prefix under which the application factory mounts the router.
        """
        super().__init__(dependencies=dependencies, include_in_schema=include_in_schema)
        self.diracx_require_auth = require_auth
        self.diracx_path_root = path_root

    ####
    # These 2 methods are needed to overwrite routes
    # https://github.com/tiangolo/fastapi/discussions/8489

    def add_api_route(self, path: str, endpoint: Callable[..., Any], **kwargs):
        """Register an auto-injected route, replacing a matching existing route.

        A route is considered a match when its path and HTTP methods are equal.

        Args:
            path: Route path relative to this router.
            endpoint: Callable handling requests to the route.
            **kwargs: Additional route options forwarded to ``APIRouter``.

        Returns:
            The registered API route.
        """
        endpoint = auto_inject(endpoint)

        routes = _without_overridden_route(
            self.routes, self.prefix + path, set(kwargs.get("methods", []))
        )
        if routes is not None:
            self.routes[:] = routes
            # Invalidate the routes FastAPI caches for the included routers
            self._mark_routes_changed()

        return super().add_api_route(path, endpoint, **kwargs)

    ######
