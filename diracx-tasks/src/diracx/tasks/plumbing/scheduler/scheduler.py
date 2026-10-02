from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from opentelemetry import metrics
from redis.asyncio import BlockingConnectionPool, Redis, ResponseError

from .._redis_types import MessageTransport
from ..base_task import BaseTask, PeriodicBaseTask, PeriodicVoAwareBaseTask
from ..broker._types import _BlockingConnectionPool
from ..broker.models import TaskMessage, submit_task
from ..broker.redis_streams import ALL_STREAM_NAMES, RedisStreamBroker

if TYPE_CHECKING:
    from diracx.core.config import Config

    from ..persistence.dlq import TaskDB

logger = logging.getLogger(__name__)
_meter = metrics.get_meter(__name__)

_periodic_submissions = _meter.create_counter(
    "scheduler_periodic_submissions_total",
    description="Periodic tasks submitted by the scheduler (outcome: success or failure)",
)
_delayed_promoted = _meter.create_counter(
    "delayed_tasks_promoted_total",
    description="Delayed tasks (including retries) moved from the delayed ZSET to their stream",
)

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


@dataclass(frozen=True)
class StreamStats:
    """State of a task stream, as seen by the workers consumer group."""

    length: int
    # Delivered to a worker but not acknowledged yet (i.e. in progress)
    pending: int | None
    # Not delivered to any worker yet (i.e. backlog)
    lag: int | None


async def collect_stream_stats(
    redis: Redis, consumer_group_name: str
) -> dict[str, StreamStats]:
    """Return the length, pending and lag of each of the task streams."""
    stats = {}
    for stream in ALL_STREAM_NAMES:
        length = await redis.xlen(stream)
        pending = lag = None
        try:
            groups = await redis.xinfo_groups(stream)
        except ResponseError:
            # The stream does not exist yet
            groups = []
        for group in groups:
            name = group["name"]
            if isinstance(name, bytes):
                name = name.decode()
            if name == consumer_group_name:
                pending = group.get("pending")
                # ``lag`` is only available with Redis >= 7,
                # and is None when it cannot be computed (e.g. after trimming)
                lag = group.get("lag")
        stats[stream] = StreamStats(length=length, pending=pending, lag=lag)
    return stats


def _stream_attributes(stream: str) -> dict[str, str]:
    # Stream names are diracx:tasks:<priority>:<size>
    _, _, priority, size = stream.split(":")
    return {"stream": stream, "priority": priority, "size": size}


# All instances in the process, so that the metrics are reported
# by module level instruments (instruments cannot be removed once created)
_schedulers: weakref.WeakSet[TaskScheduler] = weakref.WeakSet()


def _observe_leader(
    options: metrics.CallbackOptions,
) -> Iterable[metrics.Observation]:
    for scheduler in list(_schedulers):
        yield metrics.Observation(int(scheduler.is_leader))


def _observe_delayed_count(
    options: metrics.CallbackOptions,
) -> Iterable[metrics.Observation]:
    for scheduler in list(_schedulers):
        if scheduler.is_leader:
            yield metrics.Observation(scheduler._delayed_zset_size)


def _make_stream_observer(field: str):
    def _observe(
        options: metrics.CallbackOptions,
    ) -> Iterable[metrics.Observation]:
        for scheduler in list(_schedulers):
            if not scheduler.is_leader:
                continue
            for stream, stats in scheduler._stream_stats.items():
                value = getattr(stats, field)
                if value is not None:
                    yield metrics.Observation(value, _stream_attributes(stream))

    return _observe


def _observe_dlq(
    options: metrics.CallbackOptions,
) -> Iterable[metrics.Observation]:
    for scheduler in list(_schedulers):
        if not scheduler.is_leader:
            continue
        for (task_name, status), count in scheduler._dlq_counts.items():
            yield metrics.Observation(count, {"task_name": task_name, "status": status})


_meter.create_observable_gauge(
    "dead_letter_queue_tasks",
    callbacks=[_observe_dlq],
    description="Tasks in the dead letter queue, per task and status (requires the TaskDB)",
)
_meter.create_observable_gauge(
    "scheduler_leader",
    callbacks=[_observe_leader],
    description="1 if this scheduler instance holds the singleton lock, 0 otherwise",
)
_meter.create_observable_gauge(
    "delayed_tasks_pending",
    callbacks=[_observe_delayed_count],
    description="Number of tasks waiting in the delayed ZSET",
)
_meter.create_observable_gauge(
    "task_stream_length",
    callbacks=[_make_stream_observer("length")],
    description="Number of entries in the task stream",
)
_meter.create_observable_gauge(
    "task_stream_pending",
    callbacks=[_make_stream_observer("pending")],
    description="Messages delivered to a worker but not acknowledged yet",
)
_meter.create_observable_gauge(
    "task_stream_lag",
    callbacks=[_make_stream_observer("lag")],
    description="Messages in the task stream not delivered to any worker yet",
)


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
        stream_stats_interval: float = 10.0,
        dlq_stats_interval: float = 60.0,
        task_db: TaskDB | None = None,
        delayed_batch_size: int = 100,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        self.broker = broker
        self.prefix = prefix
        self.check_interval = check_interval
        self.delayed_poll_interval = delayed_poll_interval
        self.config_watch_interval = config_watch_interval
        self.stream_stats_interval = stream_stats_interval
        self.dlq_stats_interval = dlq_stats_interval
        self.task_db = task_db
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
        # Cached values for the OTel observable gauges
        self.is_leader = False
        self._delayed_zset_size: int = 0
        self._stream_stats: dict[str, StreamStats] = {}
        self._dlq_counts: dict[tuple[str, str], int] = {}
        _schedulers.add(self)

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
        self.is_leader = True

        periodic_task = asyncio.create_task(self._periodic_loop(_finish))
        delayed_task = asyncio.create_task(self._delayed_poll_loop(_finish))
        lock_task = asyncio.create_task(self._lock_extend_loop(_finish))
        config_task = asyncio.create_task(self._config_watch_loop(_finish))
        stats_task = asyncio.create_task(self._stream_stats_loop(_finish))
        loops = [periodic_task, delayed_task, lock_task, config_task, stats_task]
        if self.task_db is not None:
            loops.append(asyncio.create_task(self._dlq_stats_loop(_finish)))

        try:
            await asyncio.gather(*loops)
        finally:
            self.is_leader = False
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
                        _delayed_promoted.add(promoted)
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

    async def _stream_stats_loop(self, finish_event: asyncio.Event) -> None:
        """Periodically collect the state of the task streams for the metrics.

        This is done by the scheduler rather than by the workers, as it is
        a singleton: the values are reported exactly once.
        """
        async with Redis(connection_pool=self.connection_pool) as redis:
            while not finish_event.is_set():
                try:
                    self._stream_stats = await collect_stream_stats(
                        redis, self.broker.consumer_group_name
                    )
                except Exception:
                    logger.exception("Error collecting task stream statistics")

                try:
                    await asyncio.wait_for(
                        finish_event.wait(), timeout=self.stream_stats_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass

    async def collect_dlq_stats(self) -> None:
        """Count the tasks of the dead letter queue, for the metrics."""
        assert self.task_db is not None
        async with self.task_db:
            counts = await self.task_db.count_dlq_tasks()
        # Report 0 for what is not there anymore, rather than stopping to
        # report it, so that the time series go down to 0
        self._dlq_counts = {**dict.fromkeys(self._dlq_counts, 0), **counts}

    async def _dlq_stats_loop(self, finish_event: asyncio.Event) -> None:
        """Periodically count the tasks of the dead letter queue."""
        while not finish_event.is_set():
            try:
                await self.collect_dlq_stats()
            except Exception:
                logger.exception("Error collecting dead letter queue statistics")

            try:
                await asyncio.wait_for(
                    finish_event.wait(), timeout=self.dlq_stats_interval
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
        return list(self._config.registry)

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
            _periodic_submissions.add(
                1, attributes={"task_name": task_name, "outcome": "success"}
            )
            logger.info("Submitted periodic task %s (vo=%s)", task_name, vo or "N/A")
        except Exception:
            _periodic_submissions.add(
                1, attributes={"task_name": task_name, "outcome": "failure"}
            )
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
                        self.is_leader = False
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
