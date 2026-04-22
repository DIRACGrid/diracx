from __future__ import annotations

__all__ = ["TaskScheduler"]

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from opentelemetry import metrics
from redis.asyncio import BlockingConnectionPool, Redis

from .._redis_types import MessageTransport
from ..base_task import BaseTask, PeriodicBaseTask, PeriodicVoAwareBaseTask
from ..broker._types import _BlockingConnectionPool
from ..broker.models import TaskMessage, submit_task
from ..broker.redis_streams import RedisStreamBroker

if TYPE_CHECKING:
    from diracx.core.config import Config

logger = logging.getLogger(__name__)
_meter = metrics.get_meter(__name__)

SCHEDULER_LOCK_KEY = "diracx:scheduler:lock"
SCHEDULER_LOCK_TTL_SECONDS = 30

# Lua script: release the scheduler lock only if we still own it
_RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Lua script: extend the scheduler lock TTL only if we still own it
_EXTEND_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Lua script for atomic delayed-task promotion.
# For each due member: remove from ZSET, deserialize with cmsgpack to
# extract priority+size labels, and XADD directly to the target stream.
# Everything happens in a single atomic Lua call — no window where a
# crash could lose tasks between ZSET removal and stream insertion.
_PROMOTE_DELAYED_SCRIPT = """
local members = redis.call("zrangebyscore", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
local promoted = 0
for i, member in ipairs(members) do
    redis.call("zrem", KEYS[1], member)
    local msg = cmsgpack.unpack(member)
    local labels = msg["labels"] or {}
    local priority = labels["priority"] or "normal"
    local size = labels["size"] or "medium"
    local stream = "diracx:tasks:" .. priority .. ":" .. size
    redis.call("xadd", stream, "*", "data", member)
    promoted = promoted + 1
end
return promoted
"""

DELAYED_ZSET_KEY = "diracx:tasks:delayed"
SCHEDULE_DUMP_INTERVAL_SECONDS = 600
SCHEDULE_DUMP_MAX_ENTRIES = 20


async def schedule_delayed(
    redis: MessageTransport,
    message: TaskMessage,
    run_at: datetime,
) -> None:
    """Add a task to the delayed ZSET for future execution."""
    await redis.zadd(
        DELAYED_ZSET_KEY,
        {message.dumpb(): run_at.timestamp()},
    )


class TaskScheduler:
    """Scheduler managing periodic tasks and delayed ZSET polling.

    Runs as a singleton StatefulSet (1 replica) with a Redis mutex
    as defense-in-depth.

    Responsibilities:
      1. Load periodic task definitions from entry points + config
      2. Track next occurrence for each periodic task; submit when due
      3. Poll the delayed ZSET for tasks whose time has come
      4. Watch config for schedule changes
    """

    def __init__(
        self,
        broker: RedisStreamBroker,
        redis_url: str,
        *,
        task_registry: dict[str, type[BaseTask]] | None = None,
        config: Config | None = None,
        prefix: str = "diracx:scheduler",
        check_interval: float = 10.0,
        delayed_poll_interval: float = 1.0,
        config_watch_interval: float = 60.0,
        delayed_batch_size: int = 100,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        self.broker = broker
        self.prefix = prefix
        self.check_interval = check_interval
        self.delayed_poll_interval = delayed_poll_interval
        self.config_watch_interval = config_watch_interval
        self.delayed_batch_size = delayed_batch_size
        self.task_registry = task_registry or {}
        self._config = config
        self._instance_id = uuid4().hex
        self.connection_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
            url=redis_url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        # Mapping of (task_class_name, vo_or_empty) -> next_scheduled_time
        self._next_runs: dict[tuple[str, str], datetime] = {}
        self._schedule_dump_interval_seconds = SCHEDULE_DUMP_INTERVAL_SECONDS
        self._last_schedule_dump_at: datetime | None = None
        # Cached ZSET size for OTel observable gauge
        self._delayed_zset_size: int = 0
        _meter.create_observable_gauge(
            "delayed_tasks_pending",
            callbacks=[self._observe_delayed_count],
            description="Number of tasks waiting in the delayed ZSET",
        )

    async def startup(self) -> None:
        await self.broker.startup()
        self._log_task_registry_awareness()
        logger.info("Scheduler started")

    async def shutdown(self) -> None:
        await self.broker.shutdown()
        await self.connection_pool.disconnect()
        logger.info("Scheduler shut down")

    async def run_forever(self, finish_event: asyncio.Event | None = None) -> None:
        """Run the scheduler loops concurrently.

        Acquires a Redis mutex as defense-in-depth (on top of k8s
        StatefulSet ensuring a single replica).  If the lock cannot
        be acquired, waits and retries.
        """
        _finish = finish_event or asyncio.Event()

        # Defense-in-depth: acquire scheduler singleton lock
        while not _finish.is_set():
            if await self._acquire_scheduler_lock():
                break
            logger.warning(
                "Another scheduler holds the lock, retrying in %ds",
                SCHEDULER_LOCK_TTL_SECONDS,
            )
            try:
                await asyncio.wait_for(
                    _finish.wait(), timeout=SCHEDULER_LOCK_TTL_SECONDS
                )
                return  # finish_event was set while waiting
            except asyncio.TimeoutError:
                pass

        logger.info("Acquired scheduler lock (instance=%s)", self._instance_id)

        periodic_task = asyncio.create_task(self._periodic_loop(_finish))
        delayed_task = asyncio.create_task(self._delayed_poll_loop(_finish))
        lock_task = asyncio.create_task(self._lock_extend_loop(_finish))
        config_task = asyncio.create_task(self._config_watch_loop(_finish))

        try:
            await asyncio.gather(periodic_task, delayed_task, lock_task, config_task)
        finally:
            await self._release_scheduler_lock()

    async def _periodic_loop(self, finish_event: asyncio.Event) -> None:
        """Check periodic tasks and submit them when due."""
        # Initialize next-run times
        self._compute_initial_schedules()

        while not finish_event.is_set():
            now = datetime.now(tz=UTC)

            coros = []
            due_updates: dict[tuple[str, str], datetime] = {}
            for (task_name, vo), next_run in list(self._next_runs.items()):
                if now >= next_run:
                    coros.append(self._submit_periodic_task(task_name, vo))
                    task_cls = self.task_registry.get(task_name)
                    if task_cls and hasattr(task_cls, "default_schedule"):
                        due_updates[(task_name, vo)] = (
                            task_cls.default_schedule.next_occurrence()
                        )

            if coros:
                await asyncio.gather(*coros)

            self._next_runs.update(due_updates)
            if self._should_dump_schedule_snapshot(now):
                self._log_next_schedules_snapshot("periodic")
                self._last_schedule_dump_at = now

            try:
                await asyncio.wait_for(finish_event.wait(), timeout=self.check_interval)
                break
            except asyncio.TimeoutError:
                pass

    async def _delayed_poll_loop(self, finish_event: asyncio.Event) -> None:
        """Poll the delayed ZSET and promote due tasks to streams.

        Promotion is fully atomic inside a Lua script: for each due
        member the script removes it from the ZSET, deserialises it
        with cmsgpack to read the target stream, and XADDs it — all
        in one call.  No tasks can be lost to a crash mid-promotion.
        """
        async with Redis(connection_pool=self.connection_pool) as redis:
            while not finish_event.is_set():
                try:
                    now_ts = datetime.now(tz=UTC).timestamp()
                    promoted = await redis.eval(  # type: ignore[arg-type]
                        _PROMOTE_DELAYED_SCRIPT,
                        1,
                        DELAYED_ZSET_KEY,
                        str(now_ts),
                        str(self.delayed_batch_size),
                    )
                    if promoted:
                        logger.debug("Promoted %d delayed tasks to streams", promoted)

                    self._delayed_zset_size = await redis.zcard(DELAYED_ZSET_KEY)
                except Exception:
                    logger.exception("Error in delayed poll loop")

                try:
                    await asyncio.wait_for(
                        finish_event.wait(), timeout=self.delayed_poll_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass

    def load_vos(self) -> list[str]:
        """Load the list of VOs from the DiracX configuration.

        Reads from the Config object's Registry section, where each
        key is a VO name.
        """
        if self._config is None:
            logger.warning("No config available, cannot load VOs")
            return []
        return list(self._config.Registry)

    def _compute_initial_schedules(self) -> None:
        """Compute the initial next-run times for all periodic tasks."""
        vos = self.load_vos()
        non_periodic_count = 0
        disabled_count = 0
        periodic_count = 0
        vo_aware_count = 0
        scheduled_entries = 0

        for task_name, task_cls in self.task_registry.items():
            if not issubclass(task_cls, PeriodicBaseTask):
                non_periodic_count += 1
                continue
            periodic_count += 1
            if not getattr(task_cls, "_enabled", True):
                disabled_count += 1
                continue
            schedule = task_cls.default_schedule

            if issubclass(task_cls, PeriodicVoAwareBaseTask):
                vo_aware_count += 1
                if not vos:
                    logger.warning(
                        "No VOs configured, skipping VO-aware task %s",
                        task_name,
                    )
                    continue
                for vo in vos:
                    self.add_vo_schedule(task_name, vo, schedule.next_occurrence())
                    scheduled_entries += 1
            else:
                self._next_runs[(task_name, "")] = schedule.next_occurrence()
                scheduled_entries += 1

        logger.info(
            "Initial periodic schedules computed: entries=%d periodic=%d "
            "vo_aware=%d disabled=%d non_periodic=%d vos=%d",
            scheduled_entries,
            periodic_count,
            vo_aware_count,
            disabled_count,
            non_periodic_count,
            len(vos),
        )
        self._log_next_schedules_snapshot("initial")

    def add_vo_schedule(self, task_name: str, vo: str, next_run: datetime) -> None:
        """Register a VO-specific periodic task schedule."""
        self._next_runs[(task_name, vo)] = next_run

    async def _submit_periodic_task(self, task_name: str, vo: str) -> None:
        """Submit a periodic task to the broker."""
        task_cls = self._find_task_class(task_name)
        if task_cls is None:
            logger.warning("Task class %r not found", task_name)
            return

        labels: dict[str, Any] = {
            "priority": task_cls.priority,
            "size": task_cls.size,
            "periodic": True,
        }
        args: list[Any] = []

        if issubclass(task_cls, PeriodicVoAwareBaseTask) and vo:
            labels["vo"] = vo
            # VO is the first constructor argument for VO-aware tasks
            args.append(vo)

        try:
            await submit_task(
                broker=self.broker,
                task_name=task_name,
                task_args=args,
                labels=labels,
            )
            logger.info("Submitted periodic task %s (vo=%s)", task_name, vo or "N/A")
        except Exception:
            logger.exception(
                "Failed to submit periodic task %s (vo=%s)", task_name, vo or "N/A"
            )

    def _find_task_class(self, task_name: str) -> type[BaseTask] | None:
        return self.task_registry.get(task_name)

    # ------------------------------------------------------------------
    # Redis singleton mutex (defense-in-depth)
    # ------------------------------------------------------------------

    async def _acquire_scheduler_lock(self) -> bool:
        """Try to acquire the scheduler singleton lock via SET NX."""
        async with Redis(connection_pool=self.connection_pool) as redis:
            return bool(
                await redis.set(
                    SCHEDULER_LOCK_KEY,
                    self._instance_id,
                    nx=True,
                    ex=SCHEDULER_LOCK_TTL_SECONDS,
                )
            )

    async def _release_scheduler_lock(self) -> None:
        """Release the lock only if we still own it (atomic via Lua)."""
        async with Redis(connection_pool=self.connection_pool) as redis:
            await redis.eval(  # type: ignore[arg-type]
                _RELEASE_LOCK_SCRIPT, 1, SCHEDULER_LOCK_KEY, self._instance_id
            )

    async def _lock_extend_loop(self, finish_event: asyncio.Event) -> None:
        """Periodically extend the scheduler lock TTL (atomic via Lua)."""
        interval = SCHEDULER_LOCK_TTL_SECONDS / 3
        async with Redis(connection_pool=self.connection_pool) as redis:
            while not finish_event.is_set():
                try:
                    result = await redis.eval(  # type: ignore[arg-type]
                        _EXTEND_LOCK_SCRIPT,
                        1,
                        SCHEDULER_LOCK_KEY,
                        self._instance_id,
                        str(SCHEDULER_LOCK_TTL_SECONDS),
                    )
                    if not result:
                        logger.error("Lost scheduler lock, shutting down")
                        finish_event.set()
                        return
                except Exception:
                    logger.exception("Error extending scheduler lock")

                try:
                    await asyncio.wait_for(finish_event.wait(), timeout=interval)
                    break
                except asyncio.TimeoutError:
                    pass

    # ------------------------------------------------------------------
    # Config watch
    # ------------------------------------------------------------------

    async def _config_watch_loop(self, finish_event: asyncio.Event) -> None:
        """Periodically check for config changes and reconcile schedules.

        Detects added/removed VOs and updates ``_next_runs`` for
        VO-aware periodic tasks accordingly.
        """
        known_vos: set[str] = set(self.load_vos())

        while not finish_event.is_set():
            try:
                await asyncio.wait_for(
                    finish_event.wait(), timeout=self.config_watch_interval
                )
                break
            except asyncio.TimeoutError:
                pass

            current_vos = set(self.load_vos())
            if current_vos == known_vos:
                continue

            added = current_vos - known_vos
            removed = known_vos - current_vos
            known_vos = current_vos

            if added:
                logger.info("New VOs detected: %s", added)
            if removed:
                logger.info("Removed VOs detected: %s", removed)

            # Add schedules for new VOs
            for task_name, task_cls in self.task_registry.items():
                if not issubclass(task_cls, PeriodicVoAwareBaseTask):
                    continue
                if not getattr(task_cls, "_enabled", True):
                    continue
                for vo in added:
                    self.add_vo_schedule(
                        task_name, vo, task_cls.default_schedule.next_occurrence()
                    )

            # Remove schedules for removed VOs
            for key in list(self._next_runs):
                if key[1] in removed:
                    del self._next_runs[key]

            logger.info(
                "Reconciled VO schedules: tracked_entries=%d added_vos=%d removed_vos=%d",
                len(self._next_runs),
                len(added),
                len(removed),
            )
            self._log_next_schedules_snapshot("config_reconcile")

    def _log_task_registry_awareness(self) -> None:
        """Log which tasks are known to the scheduler."""
        periodic_enabled: list[str] = []
        vo_aware_enabled: list[str] = []
        disabled_periodic: list[str] = []
        non_periodic: list[str] = []

        for task_name, task_cls in self.task_registry.items():
            if not issubclass(task_cls, PeriodicBaseTask):
                non_periodic.append(task_name)
                continue
            if not getattr(task_cls, "_enabled", True):
                disabled_periodic.append(task_name)
                continue
            periodic_enabled.append(task_name)
            if issubclass(task_cls, PeriodicVoAwareBaseTask):
                vo_aware_enabled.append(task_name)

        periodic_enabled.sort()
        vo_aware_enabled.sort()
        disabled_periodic.sort()
        non_periodic.sort()

        logger.info(
            "Scheduler task registry: total=%d periodic_enabled=%d "
            "vo_aware_enabled=%d periodic_disabled=%d non_periodic=%d",
            len(self.task_registry),
            len(periodic_enabled),
            len(vo_aware_enabled),
            len(disabled_periodic),
            len(non_periodic),
        )
        if periodic_enabled:
            logger.info("Scheduler periodic tasks: %s", periodic_enabled)
        if vo_aware_enabled:
            logger.info("Scheduler VO-aware periodic tasks: %s", vo_aware_enabled)
        if disabled_periodic:
            logger.info("Scheduler disabled periodic tasks: %s", disabled_periodic)
        if non_periodic:
            logger.info("Scheduler non-periodic tasks in registry: %s", non_periodic)

    def _should_dump_schedule_snapshot(self, now: datetime) -> bool:
        if self._last_schedule_dump_at is None:
            return True
        elapsed = (now - self._last_schedule_dump_at).total_seconds()
        return elapsed >= self._schedule_dump_interval_seconds

    def _log_next_schedules_snapshot(self, source: str) -> None:
        """Log a bounded, sorted dump of upcoming schedules."""
        if not self._next_runs:
            logger.info("Next schedule snapshot (%s): no tracked schedules", source)
            return

        upcoming = sorted(self._next_runs.items(), key=lambda item: item[1])
        shown = upcoming[:SCHEDULE_DUMP_MAX_ENTRIES]
        rendered = [
            {
                "task": task_name,
                "vo": vo or "N/A",
                "next_run": next_run.isoformat(),
            }
            for (task_name, vo), next_run in shown
        ]

        logger.info(
            "Next schedule snapshot (%s): tracked=%d shown=%d entries=%s",
            source,
            len(upcoming),
            len(rendered),
            rendered,
        )

    # ------------------------------------------------------------------
    # OTel observable gauge callback
    # ------------------------------------------------------------------

    def _observe_delayed_count(
        self, options: metrics.CallbackOptions
    ) -> list[metrics.Observation]:
        return [metrics.Observation(self._delayed_zset_size)]
