"""CLI entry point for interactive task execution.

Usage:
    diracx-task-run call <entry_point> [--args JSON] [--kwargs JSON] [--debugger {none,before,exception}]
    diracx-task-run submit <entry_point> [--args JSON] [--kwargs JSON] [--redis-url URL]
    diracx-task-run worker [--max-concurrent-tasks N] [--redis-url URL]
    diracx-task-run scheduler [--redis-url URL]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import traceback
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from .plumbing._redis_types import LockCoordinator

DEFAULT_REDIS_URL = "redis://localhost"
REDIS_URL_ENV_VAR = "DIRACX_TASKS_REDIS_URL"


class DebugOptions(StrEnum):
    NONE = "none"
    BEFORE = "before"
    ON_ERROR = "exception"


def _get_redis_url(args: argparse.Namespace) -> str:
    """Resolve Redis URL from CLI arg, env var, or default."""
    if hasattr(args, "redis_url") and args.redis_url:
        return args.redis_url
    return os.environ.get(REDIS_URL_ENV_VAR, DEFAULT_REDIS_URL)


def main() -> None:
    """Parse arguments and dispatch to the appropriate subcommand."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="DiracX tasks CLI", allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)

    # call subcommand
    call_parser = subparsers.add_parser("call", help="Execute a task interactively")
    call_parser.add_argument("entry_point", help="Task entry point name")
    call_parser.add_argument(
        "--args",
        default=[],
        type=json.loads,
        help="JSON list of positional arguments (default: '[]')",
    )
    call_parser.add_argument(
        "--kwargs",
        default={},
        type=json.loads,
        help="JSON dict of keyword arguments (default: '{}')",
    )
    call_parser.add_argument(
        "--debugger",
        type=DebugOptions,
        default=DebugOptions.NONE,
        help="Drop into debugger: 'before' or 'exception'",
    )
    call_parser.set_defaults(
        func=lambda args: asyncio.run(
            call_task(
                args.entry_point,
                args=args.args,
                kwargs=args.kwargs,
                debugger=args.debugger,
            )
        )
    )

    # submit subcommand
    submit_parser = subparsers.add_parser(
        "submit", help="Submit a task to the broker for worker execution"
    )
    submit_parser.add_argument("entry_point", help="Task entry point name")
    submit_parser.add_argument(
        "--args",
        default=[],
        type=json.loads,
        help="JSON list of positional arguments (default: '[]')",
    )
    submit_parser.add_argument(
        "--kwargs",
        default={},
        type=json.loads,
        help="JSON dict of keyword arguments (default: '{}')",
    )
    submit_parser.add_argument(
        "--redis-url",
        type=str,
        default=None,
        help=f"Redis URL (default: ${REDIS_URL_ENV_VAR} or {DEFAULT_REDIS_URL})",
    )
    submit_parser.set_defaults(
        func=lambda args: asyncio.run(
            submit_task_cli(
                args.entry_point,
                args=args.args,
                kwargs=args.kwargs,
                redis_url=_get_redis_url(args),
            )
        )
    )

    # worker subcommand
    worker_parser = subparsers.add_parser("worker", help="Start a task worker")
    worker_parser.add_argument(
        "--max-concurrent-tasks",
        type=int,
        required=True,
        help="Maximum number of tasks to run concurrently (default: 10)",
    )
    worker_parser.add_argument(
        "--redis-url",
        type=str,
        default=None,
        help=f"Redis URL (default: ${REDIS_URL_ENV_VAR} or {DEFAULT_REDIS_URL})",
    )
    worker_parser.add_argument(
        "--worker-size",
        type=str,
        required=True,
        choices=["small", "medium", "large"],
        help="Worker size determining which task streams to listen on (default: medium)",
    )
    worker_parser.set_defaults(
        func=lambda args: asyncio.run(
            start_worker(
                redis_url=_get_redis_url(args),
                max_concurrent_tasks=args.max_concurrent_tasks,
                worker_size=args.worker_size,
            )
        )
    )

    # scheduler subcommand
    scheduler_parser = subparsers.add_parser(
        "scheduler", help="Start the task scheduler"
    )
    scheduler_parser.add_argument(
        "--redis-url",
        type=str,
        default=None,
        help=f"Redis URL (default: ${REDIS_URL_ENV_VAR} or {DEFAULT_REDIS_URL})",
    )
    scheduler_parser.set_defaults(
        func=lambda args: asyncio.run(start_scheduler(redis_url=_get_redis_url(args)))
    )

    parsed = parser.parse_args()
    parsed.func(parsed)


async def start_worker(
    redis_url: str,
    max_concurrent_tasks: int,
    worker_size: str,
) -> None:
    """Start a worker to execute tasks from the broker."""
    from .plumbing.broker import RedisStreamBroker
    from .plumbing.enums import Size
    from .plumbing.factory import (
        BaseTask,
        create_task_bindings,
        load_task_registry,
        setup_dependency_overrides,
    )
    from .plumbing.worker import Worker

    size = Size(worker_size)
    broker = RedisStreamBroker(url=redis_url, worker_size=size)
    task_classes = load_task_registry()
    task_bindings, wrapped_registry = create_task_bindings(broker, task_classes)
    BaseTask.bind_broker(task_bindings)

    # Collect all task dependants for settings discovery
    dependants = [
        f._dependant for f in wrapped_registry.values() if hasattr(f, "_dependant")
    ]

    async with setup_dependency_overrides(task_dependants=dependants) as overrides:
        broker.dependency_overrides.update(overrides)

        task_db = None
        task_db_url = os.environ.get("DIRACX_DB_URL_TASKDB")
        if task_db_url:
            from .plumbing.persistence.dlq import TaskDB

            task_db = TaskDB(task_db_url)

        finish_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, finish_event.set)

        if task_db:
            async with task_db.engine_context():
                worker = Worker(
                    broker=broker,
                    task_registry=wrapped_registry,
                    task_class_registry=task_classes,
                    max_concurrent_tasks=max_concurrent_tasks,
                    task_db=task_db,
                )
                await worker.listen(finish_event)
        else:
            worker = Worker(
                broker=broker,
                task_registry=wrapped_registry,
                task_class_registry=task_classes,
                max_concurrent_tasks=max_concurrent_tasks,
            )
            await worker.listen(finish_event)


async def start_scheduler(redis_url: str) -> None:
    """Start the task scheduler process."""
    from .plumbing.broker import RedisStreamBroker
    from .plumbing.factory import load_task_registry
    from .plumbing.scheduler import TaskScheduler

    broker = RedisStreamBroker(url=redis_url)
    task_classes = load_task_registry()

    config = None
    config_url = os.environ.get("DIRACX_CONFIG_BACKEND_URL")
    if config_url:
        from diracx.core.config import ConfigSource

        config_source = ConfigSource.create_from_url(backend_url=config_url)
        config = config_source.read_config()

    scheduler = TaskScheduler(
        broker=broker,
        redis_url=redis_url,
        task_registry=task_classes,
        config=config,
    )

    await scheduler.startup()
    finish_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, finish_event.set)
    try:
        await scheduler.run_forever(finish_event)
    finally:
        await scheduler.shutdown()


async def submit_task_cli(
    entry_point: str,
    args: Iterable[Any],
    kwargs: dict[str, Any],
    redis_url: str,
) -> None:
    """Submit a task to the broker for execution by workers."""
    from .plumbing.broker import RedisStreamBroker
    from .plumbing.broker.models import submit_task
    from .plumbing.factory import load_task_registry

    registry = load_task_registry()

    task_cls = registry.get(entry_point)
    if task_cls is None:
        print(f"Task {entry_point!r} not found. Available: {sorted(registry)}")
        sys.exit(1)

    broker = RedisStreamBroker(url=redis_url)
    await broker.startup()

    try:
        task_id = await submit_task(
            broker=broker,
            task_name=entry_point,
            task_args=list(args),
            task_kwargs=kwargs or None,
            labels={
                "priority": task_cls.priority,
                "size": task_cls.size,
            },
        )
        print(f"Submitted task {entry_point!r} with ID: {task_id}")
    finally:
        await broker.shutdown()


async def call_task(
    entry_point: str,
    args: Iterable[Any],
    kwargs: dict[str, Any],
    debugger: DebugOptions,
) -> None:
    """Execute a task interactively (no broker).

    Uses ``task_wrapper`` with ``_interactive=True`` so that structural
    locks (Mutex, RW) are acquired when Redis is available, while
    limiters (rate/concurrency) are skipped.

    Dependency injection (databases, config, settings) is resolved
    automatically from environment variables via
    ``setup_dependency_overrides``.
    """
    from .plumbing.factory import (
        find_missing_overrides,
        load_task_registry,
        setup_dependency_overrides,
        task_wrapper,
        wrap_task,
    )
    from .plumbing.worker.di_resolver import solve_task_dependencies

    registry = load_task_registry()

    task_cls = registry.get(entry_point)
    if task_cls is None:
        print(f"Task {entry_point!r} not found. Available: {sorted(registry)}")
        sys.exit(1)

    # Try to connect to Redis for lock acquisition
    redis: LockCoordinator | None = None
    redis_url = os.environ.get(REDIS_URL_ENV_VAR)
    if redis_url:
        from redis.asyncio import Redis

        redis = Redis.from_url(redis_url)

    wrapped = wrap_task(task_cls)

    async with setup_dependency_overrides(
        task_dependants=[wrapped._dependant],  # type: ignore[attr-defined]
    ) as overrides:
        missing = find_missing_overrides(
            wrapped._dependant,  # type: ignore[attr-defined]
            overrides,
        )
        if missing:
            print(
                f"Cannot resolve dependencies for task {entry_point!r}.\n"
                "Set the following environment variables:"
            )
            for m in missing:
                print(f"  {m}")
            sys.exit(1)

        dep_kwargs, async_exit_stack = await solve_task_dependencies(
            call=wrapped,
            dependency_overrides=overrides,
        )

        try:
            all_kwargs = {**dep_kwargs, **kwargs}

            if debugger == DebugOptions.BEFORE:
                breakpoint()  # noqa: T100

            result = await task_wrapper(
                task_cls, *args, _redis=redis, _interactive=True, **all_kwargs
            )
            print(f"Result: {result}")
        except Exception:
            if debugger != DebugOptions.ON_ERROR:
                raise
            import pdb

            traceback_info = sys.exc_info()
            traceback.print_exception(*traceback_info)
            pdb.post_mortem(traceback_info[2])
        finally:
            await async_exit_stack.aclose()
            if redis is not None:
                await redis.aclose()
