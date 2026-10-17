from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, Callable

from fastapi.concurrency import contextmanager_in_threadpool
from fastapi.dependencies.models import (
    Dependant,
    _get_cache_key,
    _is_async_gen_callable,
    _is_coroutine_callable,
    _is_gen_callable,
)
from fastapi.dependencies.utils import get_dependant
from starlette.concurrency import run_in_threadpool


async def solve_task_dependencies(
    *,
    call: Callable[..., Any],
    dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
) -> tuple[dict[str, Any], AsyncExitStack]:
    """Resolve FastAPI-style Depends() annotations for a background task.

    Unlike FastAPI's solve_dependencies, this doesn't require Request/WebSocket
    objects since we're running in a background worker.

    If the callable has a pre-computed ``_dependant`` attribute (set by
    ``wrap_task``), it is reused to avoid repeated reflection.

    Returns:
        Tuple of (resolved kwargs dict, async exit stack).

    """
    dependant = getattr(call, "_dependant", None) or get_dependant(path="/", call=call)

    dependency_cache: dict[Any, Any] = {}
    async_exit_stack = AsyncExitStack()

    values = await _resolve_dependant(
        dependant=dependant,
        dependency_overrides=dependency_overrides or {},
        dependency_cache=dependency_cache,
        async_exit_stack=async_exit_stack,
    )

    return values, async_exit_stack


async def _resolve_dependant(
    *,
    dependant: Dependant,
    dependency_overrides: dict[Callable[..., Any], Callable[..., Any]],
    dependency_cache: dict[Any, Any],
    async_exit_stack: AsyncExitStack,
) -> dict[str, Any]:
    """Recursively resolve a dependant and its sub-dependencies."""
    values: dict[str, Any] = {}

    for sub_dependant in dependant.dependencies:
        call = sub_dependant.call
        use_sub_dependant = sub_dependant

        if call in dependency_overrides:
            override_call = dependency_overrides[call]
            use_path = sub_dependant.path or "/"
            use_sub_dependant = get_dependant(
                path=use_path,
                call=override_call,
                name=sub_dependant.name,
                scope=sub_dependant.scope,
            )
            call = override_call

        # Check cache
        # Since FastAPI 0.137 the cache key is computed by _get_cache_key
        # instead of the Dependant.cache_key property
        sub_dependant_cache_key = _get_cache_key(dependant=use_sub_dependant)
        if use_sub_dependant.use_cache and sub_dependant_cache_key in dependency_cache:
            solved = dependency_cache[sub_dependant_cache_key]
        else:
            # Recursively resolve sub-dependencies
            sub_values = await _resolve_dependant(
                dependant=use_sub_dependant,
                dependency_overrides=dependency_overrides,
                dependency_cache=dependency_cache,
                async_exit_stack=async_exit_stack,
            )

            # Since FastAPI 0.137 the callable type checks are module level
            # functions instead of Dependant properties
            if _is_async_gen_callable(call):
                cm = asynccontextmanager(call)(**sub_values)
                solved = await async_exit_stack.enter_async_context(cm)
            elif _is_gen_callable(call):
                solved = await async_exit_stack.enter_async_context(
                    contextmanager_in_threadpool(call(**sub_values))
                )
            elif _is_coroutine_callable(call):
                solved = await call(**sub_values)
            else:
                solved = await run_in_threadpool(call, **sub_values)

            if use_sub_dependant.use_cache:
                dependency_cache[sub_dependant_cache_key] = solved

        if sub_dependant.name is not None:
            values[sub_dependant.name] = solved

    return values
