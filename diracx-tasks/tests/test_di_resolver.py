from __future__ import annotations

import functools
from functools import partial
from typing import Annotated

from fastapi import Depends

from diracx.tasks.plumbing.worker.di_resolver import solve_task_dependencies


def sync_dep() -> str:
    return "sync"


async def async_dep() -> str:
    return "async"


EVENTS: list[str] = []


def gen_dep():
    yield "gen"
    EVENTS.append("gen closed")


async def async_gen_dep():
    yield "async gen"
    EVENTS.append("async gen closed")


class CallableInstance:
    async def __call__(self) -> str:
        return "instance"


def decorated(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@decorated
async def decorated_async_dep() -> str:
    return "decorated"


async def partial_dep(value: str) -> str:
    return value


async def test_resolve_kinds_of_dependencies():
    EVENTS.clear()

    async def task(
        sync_value: Annotated[str, Depends(sync_dep)],
        async_value: Annotated[str, Depends(async_dep)],
        gen_value: Annotated[str, Depends(gen_dep)],
        async_gen_value: Annotated[str, Depends(async_gen_dep)],
        instance_value: Annotated[str, Depends(CallableInstance())],
        decorated_value: Annotated[str, Depends(decorated_async_dep)],
        partial_value: Annotated[str, Depends(partial(partial_dep, "partial"))],
    ):
        pass

    values, stack = await solve_task_dependencies(call=task)
    assert values == {
        "sync_value": "sync",
        "async_value": "async",
        "gen_value": "gen",
        "async_gen_value": "async gen",
        "instance_value": "instance",
        "decorated_value": "decorated",
        "partial_value": "partial",
    }
    assert EVENTS == []
    await stack.aclose()
    assert sorted(EVENTS) == ["async gen closed", "gen closed"]


CALLS: list[str] = []


async def counted() -> str:
    CALLS.append("counted")
    return "original"


async def counted_override() -> str:
    CALLS.append("override")
    return "override"


async def test_resolve_cache_and_overrides():
    CALLS.clear()

    async def task(
        first: Annotated[str, Depends(counted)],
        second: Annotated[str, Depends(counted)],
        uncached: Annotated[str, Depends(counted, use_cache=False)],
    ):
        pass

    values, stack = await solve_task_dependencies(call=task)
    await stack.aclose()
    assert values == {"first": "original", "second": "original", "uncached": "original"}
    assert CALLS == ["counted", "counted"]

    CALLS.clear()
    values, stack = await solve_task_dependencies(
        call=task, dependency_overrides={counted: counted_override}
    )
    await stack.aclose()
    assert values == {"first": "override", "second": "override", "uncached": "override"}
    assert CALLS == ["override", "override"]
