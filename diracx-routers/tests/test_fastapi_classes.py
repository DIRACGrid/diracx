from __future__ import annotations

from fastapi import FastAPI
from fastapi.routing import iter_route_contexts
from fastapi.testclient import TestClient

from diracx.routers.fastapi_classes import DiracxRouter


def _get(router: DiracxRouter, path: str):
    app = FastAPI()
    app.include_router(router)
    return TestClient(app).get(path)


def test_override_route_in_prefixed_include():
    inner = DiracxRouter()

    @inner.get("/foo")
    def inner_foo():
        return "inner"

    outer = DiracxRouter()
    outer.include_router(inner, prefix="/sub")

    @outer.get("/foo")
    def outer_foo():
        return "outer"

    # A route with the same path relative to its own router is not an override
    assert _get(outer, "/sub/foo").json() == "inner"
    assert _get(outer, "/foo").json() == "outer"

    @outer.get("/sub/foo")
    def outer_sub_foo():
        return "override"

    assert _get(outer, "/sub/foo").json() == "override"


def test_override_route_after_routes_are_listed():
    inner = DiracxRouter()

    @inner.get("/foo")
    def inner_foo():
        return "inner"

    outer = DiracxRouter()
    outer.include_router(inner)
    # Listing the routes caches them in the included router
    assert [ctx.name for ctx in iter_route_contexts(outer.routes)] == ["inner_foo"]

    @outer.get("/foo")
    def outer_foo():
        return "outer"

    assert [ctx.name for ctx in iter_route_contexts(outer.routes)] == ["outer_foo"]


def test_override_route_keeps_included_router():
    inner = DiracxRouter()

    @inner.get("/foo")
    def inner_foo():
        return "inner"

    @inner.get("/bar")
    def inner_bar():
        return "inner"

    outer = DiracxRouter()
    outer.include_router(inner)

    @outer.get("/foo")
    def outer_foo():
        return "outer"

    assert _get(outer, "/foo").json() == "outer"
    assert _get(outer, "/bar").json() == "inner"
    # The included router can be shared, e.g. by the vanilla DiracX app
    assert _get(inner, "/foo").json() == "inner"


def test_override_route_without_methods():
    router = DiracxRouter()

    @router.get("/foo")
    def get_foo():
        return "get"

    # api_route defaults to GET, like APIRoute
    @router.api_route("/foo")
    def api_route_foo():
        return "api_route"

    assert _get(router, "/foo").json() == "api_route"
    assert [ctx.name for ctx in iter_route_contexts(router.routes)] == ["api_route_foo"]
