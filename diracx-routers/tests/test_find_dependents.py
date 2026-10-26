from __future__ import annotations

from fastapi import Depends

from diracx.routers.factory import find_dependents
from diracx.routers.fastapi_classes import DiracxRouter


class Base:
    pass


def _dependency(name: str) -> type[Base]:
    def create(cls):
        pass

    return type(name, (Base,), {"create": classmethod(create)})


def test_find_dependents_of_included_routers():
    in_route, in_router, in_include, in_parent = (
        _dependency(name) for name in ("InRoute", "InRouter", "InInclude", "InParent")
    )

    child = DiracxRouter(dependencies=[Depends(in_router.create)])

    @child.get("/foo", dependencies=[Depends(in_route.create)])
    def foo():
        pass

    parent = DiracxRouter(dependencies=[Depends(in_parent.create)])
    parent.include_router(child, dependencies=[Depends(in_include.create)])

    assert set(find_dependents(parent, Base)) == {
        in_route,
        in_router,
        in_include,
        in_parent,
    }
