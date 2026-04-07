# DX-ADR-001: Tasks

## Metadata

- **Created By:** Chris Burr, Christophe Haen
- **Date:** 2026-03-11
- **Status:** Draft
- **Decision Maker(s):** Alexandre Boyer

## Abstract

DiracX tasks is a lightweight, async-first task execution framework built on Redis Streams. It replaces several DIRAC components with a unified broker/worker/scheduler model that supports prioritised queuing, distributed locking, periodic scheduling, dependency injection, dead-letter persistence, and extension via entry points. The components it replaces are:

- **Agents** — long-running processes that periodically poll databases to perform work (e.g. `SiteDirector`, `TransformationAgent`).
- **Executors / Optimisers** — reactive, push-based processors that receive tasks from a central Mind service and pass them through processing chains (e.g. the job optimisation pipeline: `JobPath` → `JobSanity` → `InputData` → `JobScheduling`).

## Motivation

DIRAC's current workload execution relies on Agents (periodic pollers) and Executors (reactive processors coordinated by Mind services). This architecture has several limitations:

- **Coupling:** Agent logic mixes scheduling, execution, locking, and retry concerns into a single `execute()` cycle, making agents difficult to test and extend. Executors add a separate push-based model with its own Mind services, task freezing, and fast-track dispatch — two fundamentally different execution paradigms for what is conceptually the same problem.
- **Complexity:** While the underlying primitive of an Agent is simple — poll a database and act — the emergent behaviour of how agents interact when running asynchronously is extremely difficult to model. This is a persistent source of race conditions in DIRAC, for example around transformation state transitions where multiple agents may concurrently add input data or modify the same transformation without explicit coordination.
- **Scaling:** Agents can typically only be scaled by partitioning their work through configuration (e.g. multiple `SiteDirector` instances each handling a subset of sites, or multiple `TransformationAgent` instances each handling different transformation types). This requires manual coordination and there is no enforcement that partitions don't overlap.
- **Latency:** Agents are fundamentally periodic, polling at a configured interval (default 120 seconds). They cannot react to external input in real time. The Executor/Mind model was introduced to address this for job optimisation, but it adds significant architectural complexity (Mind services, `ExecutorDispatcher`, task freezing, fast-track dispatch) despite being a generic framework that was only ever used for that single use case.
- **Resource overhead:** Each agent type requires a dedicated process, making it expensive to scale the number of distinct task types. The Executor model allows pooling but requires its own infrastructure (Mind services, message clients).
- **Kubernetes fit:** Long-running agent processes with internal state and configuration-based partitioning are awkward to operate in container-orchestrated environments where stateless, horizontally scalable workloads are the norm.

The task system addresses these by unifying both Agents and Executors into a single model, decomposing workload execution into independent components (broker, worker, scheduler) that are stateless, horizontally scalable, and naturally suited to distributed deployments. Tasks are plain Python classes with declarative configuration for priority, size, locking, and retries, making them easy to write, test interactively, and extend through the standard DiracX entry-point mechanism.

## Specification

### Tasks definition

```python
class BaseTask:
    priority  # BACKGROUND/NORMAL/REALTIME
    size  # SMALL/MEDIUM/LARGE
    retry_policy: RetryPolicyBase

    @property
    def execution_locks(self) -> list[BaseLock]:
        """Return a list of lock keys required by this task."""
        # Limiters are not applied by default however their presence allows them
        # to be configured via the central configuration repository.
        return [
            RateLimiter(LockedObjectType.TASK, self.__class__.__name__),
            ConcurrencyLimiter(LockedObjectType.TASK, self.__class__.__name__),
        ]

    async def execute(self):
        """Main execution logic of the task."""
```

```python
class RetryPolicyBase:
    def schedule_retry(self, attempt: int, exception: Exception) -> datetime | None:
        """Return the datetime for the next retry, or None to not retry."""
```

#### Arguments

Tasks can define additional arguments which serialized and stored in the task system.
These are passed as arguments to `BaseTask.__init__` and the subclass of `BaseTask` is expected to be serializable (e.g. by using `@dataclass` or `pydantic.BaseModel`).

```python
from dataclasses import dataclass


@dataclass
class ExampleTask(BaseTask):
    arg1: str
    arg2: int
```

```python
from pydantic import BaseModel


class AnotherExampleTask(BaseTask, BaseModel):
    arg1: str
    arg2: int
```

#### Dependency Injection

Subclasses of `BaseTask` can define additional arguments which are passed at task-execution time in a similar way to FastAPI's dependency injection system used in `diracx-routers`.
These are defined as arguments to `BaseTask.execute` and are expected to be type annotated with a DB class or settings class.
The `auto_inject_depends` function in `diracx.tasks.plumbing.depends` automatically detects `BaseSQLDB` subclasses, `BaseOSDB` subclasses, and `ServiceSettingsBase` subclasses and wraps them with the appropriate `Depends` annotation.

```python
from diracx.core.settings import AuthSettings
from diracx.db.sql import JobDB


class ExampleTask(BaseTask):
    async def execute(self, job_db: JobDB, auth_settings: AuthSettings):
        # Use job_db to perform database operations related to this task
        pass
```

This auto-detection is applied in three places:

- In tasks: called by `wrap_task` in `diracx.tasks.plumbing.factory`
- In routers: called by `DiracxRouter.add_api_route`
- In sub-dependency functions: applied via the `@auto_inject` decorator

#### Locking

```python
class LockedObjectType(str):
    """String representing the type of object being locked.

    e.g. "job", "transformation", "task", "lfn", etc.
    """
```

```python
class BaseLock:
    def __init__(self, obj: LockedObjectType, key: int | str):
        """Base class for locks.

        Args:
            obj: The type of object being locked.
            key: The key identifying the locked object.
        """
        self.obj = obj
        self.key = key

    @abstractmethod
    async def acquire(self, conn):
        ...

    @abstractmethod
    async def release(self, conn):
        ...
```

#### Limiters

A subset of locks are "limiters", which are only applied for non-interactive task execution.
At the time of writing two such limiters are foreseen:

```python
class RateLimiter:
    """Enforces a maximum number of operations within a given time window."""

    limit: int | None  # Number of allowed operations within the time window
    window_seconds: int | None
```

```python
class ConcurrencyLimiter:
    """Enforces a maximum number of concurrent operations."""

    limit: int | None  # Maximum number of concurrent operations
```

#### Periodic Tasks

Period tasks can be split into two categories:

- Tasks which run once per installation (e.g. cleaning the sandbox store)
- Tasks which run once per VO (e.g. pilot submission)

Periodic tasks are scheduled to run at specific intervals or times depending on a schedule property which is defined with this base class:

```python
class TaskScheduleBase:
    """Abstract base class for task scheduling."""

    def next_occurrence(self) -> datetime:
        """Return the datetime for the next scheduled occurrence."""
```

Initially, we foresee three implementations of this base class:

- `IntervalSecondsSchedule`: runs at fixed intervals defined in seconds (e.g. every 3600 seconds)
- `CronSchedule`: runs at specific times defined by a cron expression (e.g. every day at midnight)
- `RRuleSchedule`: runs at specific times defined by an iCalendar RFC5545 RRULE expression (e.g. every last Friday of the month at 5pm)

The `PeriodicBaseTask` extends a pydantic model such that it's arguments can be serialized with two private attributes which can be overridden by the configuration.

```python
class PeriodicBaseTask(BaseModel):
    """Base class for periodic tasks."""

    _schedule: TaskScheduleBase  # The default schedule for this task
    _enabled: bool = True  # Whether this periodic task is enabled by default

    @staticmethod
    def validate_config(config: list[TaskConfig]) -> None:
        """Validate the configuration for this periodic task.

        Raises a ValueError if the configuration is found to be invalid.
        """
        return

    @property
    def execution_locks(self) -> list[BaseLock]:
        """Periodic tasks cannot be executed concurrently unless subclasses opt-out."""
        # This intentionally does not call super() as the default limiters are
        # strictly more permissive than the schedule + mutex combination.
        return [
            MutexLock(LockedObjectType.TASK, self.__class__.__name__),
        ]
```

```python
class PeriodicVoAwareBaseTask(PeriodicBaseTask):
    """Base class for periodic tasks which are VO-aware."""

    vo: str

    @property
    def execution_locks(self) -> list[BaseLock]:
        """Periodic tasks cannot be executed concurrently unless subclasses opt-out."""
        # This intentionally does not call super() as the default limiters are
        # strictly more permissive than the schedule + mutex combination.
        return [
            MutexLock(LockedObjectType.TASK, self.__class__.__name__, self.vo),
        ]
```

```python
class SubmitPilots(PeriodicVoAwareBaseTask):
    def __init__(self, *, vo: str, ce_regex: str | None = None):
        super().__init__(vo=vo, ce_regex=ce_regex)


class SubmitPilots(PeriodicVoAwareBaseTask):
    def __init__(self, *, vo: str, ce_regex: str | None = None):
        super().__init__(vo=vo)
        self.ce_regex = re.compile(ce_regex)


class SubmitPilots(PeriodicVoAwareBaseTask):
    ce_regex: re.Pattern = re.compile(".*")
```

### Retries

Retries can originate from several sources:

- Retry policy: if a task raises an exception during execution, the retry policy is consulted to determine if and when to retry the task. This results in the tasks's execution count being incremented and an error being logged.
- Lock acquisition failure: if a task fails to acquire one of its locks, it will be retried after a delay which increases with the number of consecutive lock acquisition failures.

### Configuration

Configuration for tasks has two sources of truth:

- The properties on a task (and the recursive properties of non-primitive types such as locks and retry policies) define the default configuration for that task.
- Defaults can be overridden by a file which is stored in the central configuration repository. This file is expected to be in YAML format and follow a structure which mirrors the properties of the task classes.

Limiter defaults should provide a sensible default behaviour for installations, under the assumption that most admins might not be familiar with the task system internals.
These defaults have two contrasting goals:

- Avoid overloading systems due to too much concurrency hitting external systems (e.g. storage elements)
- Avoid tasks not being scheduled due to limiters being overly restrictive.

The configuration for periodic tasks should allow for the schedule to be overridden either for all occurrences of the task or on a per-VO basis.
If a task class is defined in both `common` and `vo-overrides.EXAMPLE_VO` then the `common` configuration is ignored.

```yaml
common:
  periodic-tasks:
    SubmitPilots:
      - args:
        ce_regex: .*
        enabled: True

  limits:
    Task:
      SubmitPilots:
        ConcurrencyLimiter:
          limit: 2
        RateLimiter:
          limit: 10
          window_seconds: 3600

    StorageElement:
       default:
          CreateFile:
            ConcurrencyLimiter:
              limit: 5
          RemoveFile:
            ConcurrencyLimiter:
              limit: 5
       "CERN-DST":
          RemoveFile:
            ConcurrencyLimiter:
              limit: 5
       "CERN-MC_DST":
          RemoveFile:
            ConcurrencyLimiter:
              limit: 5

    Transformation:
      default:
        ConcurrencyLimiter:
          limit: 10
      "12345":  # This transformation is only allowed to consume 5 slots
        ConcurrencyLimiter:
          limit: 5

vo-overrides:
  lhcb:
    periodic-tasks:
      SubmitPilots:
        - name: SubmitPilotsCERN
          schedule:
            class: RRuleSchedule
            arg: "FREQ=HOURLY;INTERVAL=2"
          args:
            ce_regex: .*.cern.ch
        - name: SubmitPilotsRAL
          schedule:
            class: CronSchedule
            arg: "0 */6 * * *"
          args:
            ce_regex: .*.ral.ac.uk
        - args:
            ce_regex: ~(*.cern.ch|*.ral.ac.uk)

  limits:
    ConcurrencyLimiter:
      Task:
        CheckPilotStatus:
          limit: 10_000
          window_seconds: 3600
```

```python
class RemoveFile(BaseTask):
    storage_element: str
    lfn: str

    def execution_locks(self) -> list[BaseLock]:
        return super().execution_locks + [
            MutexLock(LockedObjectType.LFN, self.lfn),
            ConcurrencyLimiter(
                LockedObjectType.STORAGE_ELEMENT,
                self.storage_element,
                StorageElementAction.RemoveFile,
            ),
        ]
```

### Scheduling

Only a single instance of the scheduler can be running at any given time, this is enforced both as a `StatefulSet` in kubernetes and with a mutex lock in the redis instance itself.
The scheduler serves two purposes:

- Scheduling tasks for execution by adding them to the pending tasks stream at the appropriate time.
- Adding periodic tests to the LIST of scheduled tasks at startup and when their schedule changes. Each time a periodic task is moved to the pending tasks stream, the next occurrence is immediately scheduled.

When the periodic task configuration is updated, the scheduler should remove the task from the pending tasks LIST if it's already there and add it again in the appropriate key.
Some additional metadata should be stored in the broker to allow for the scheduler to determine if a task needs to be rescheduled when the configuration changes (e.g. schedule, next occurrence, etc).

### Broker

The pending tasks is implemented as nine Redis streams, one per priority (BACKGROUND, NORMAL, REALTIME) and size (SMALL, MEDIUM, LARGE) combination.
This allows us to have three classes of workers which can be scaled independently and have different resource requirements, each consuming from the appropriate three priority streams.
The task scheduler is responsible for adding tasks to the appropriate stream based on their priority and size.

The state of the broker should be ephemeral and recreated with each update. Any persistent state should be stored in the standard MySQL database's used by DiracX. This requirement is imposed to:

- Simplify recovery from unexpected outages.
- Reduce the complexity of reasoning about updates which may change details of the broker's internal state.
- Improve performance by removing the need to ensure every action is flushed to persistent storage.

Upon first start:

- broker startup entry points are called. These exist to allow databases which have track task-dependent states to be reset. For example, before jobs are eligible to run tasks must be ran to assign them to sites and perform sanity checks. Jobs which have an in-flight task are assigned a specific state (e.g. `PENDING`). Upon startup, any jobs in this state are reset to `RECEIVED` and the pending tasks for these jobs are cleared from the broker. This allows the system to recover from unexpected outages without manual intervention. This means that the state machine of DiracX objects must be designed with this behaviour in mind.
- the broker is populated with the periodic tasks

## Rationale

### `Limiter` vs `Lock`

The specification distinguishes between locks (always enforced) and limiters (only enforced during non-interactive execution). This separation exists because tasks may be executed interactively, e.g. during development or debugging, where an administrator deliberately wants to bypass throttling. A `MutexLock` on an LFN must always be enforced because concurrent mutations would corrupt state regardless of context. A `RateLimiter` on a storage element exists to protect the external system from overload, which is irrelevant when an operator is manually running a single task to investigate an issue. Making this distinction explicit in the type hierarchy rather than a flag on BaseLock means the enforcement policy is visible in the class definition and cannot be accidentally bypassed or forgotten.

### `ConcurrencyLimiter` vs `RateLimiter`

Both are limiters but they protect against different failure modes. A `ConcurrencyLimiter` is stateful, it tracks tasks that are currently executing and only releases a slot when a task completes. This makes it effective for long-running operations: if a storage element becomes slow, in-flight tasks hold their slots longer, naturally applying backpressure and preventing new tasks from piling on. A `RateLimiter` is stateless with respect to in-flight work, it counts executions within a time window regardless of whether previous tasks have completed. This makes it appropriate for protecting systems where our tasks complete quickly but trigger asynchronous work on the remote side, for example, submitting file transfers where the task returns once the request is accepted but the external service continues processing. A concurrency limiter would release the slot immediately, allowing unbounded submissions that overwhelm the remote system. Tasks can declare both when needed, for example, a concurrency limit to apply backpressure when our operations are slow and a rate limit to cap the rate at which we submit work to external systems.

### `MutexLock` for Periodic Tasks

Periodic tasks default to a `MutexLock` on their class name (or class name + VO for VO-aware tasks) rather than the concurrency/rate limiters that regular tasks receive. This is because periodic tasks are typically "sweep the world" operations — they scan a database and act on everything they find. Running two instances concurrently would either duplicate work or require the task itself to handle coordination, which is the complexity the locking system exists to avoid. The mutex is scoped per-class (or per-class-per-VO) so that different periodic task types can still run concurrently with each other. Subclasses can opt out if their specific workload is safe to parallelise.

### Why three sizes / three priorities?

The three size classes (`SMALL`, `MEDIUM`, `LARGE`) exist to allow independent worker scaling with different resource allocations — a worker consuming small tasks can run on a pod with minimal memory, while large tasks may need significantly more. The three priority levels (`BACKGROUND`, `NORMAL`, `REALTIME`) ensure that latency-sensitive work (e.g. job optimisation triggered by a user submission) is not blocked behind bulk background work (e.g. accounting aggregation). Using separate streams rather than a single stream with metadata-based routing means workers only consume from streams matching their size class, and within that class always drain higher-priority streams first.

### Dependency Injection

DB classes, OS DB classes, and `ServiceSettingsBase` subclasses are auto-detected by `auto_inject_depends` in `diracx.tasks.plumbing.depends`. This means route handlers, task `execute()` methods, and sub-dependency functions can simply type-annotate their parameters with the bare class (e.g. `job_db: JobDB`) and the framework wraps them with the appropriate `Depends` call automatically.

`diracx.routers.dependencies` re-exports from `diracx.tasks.plumbing.depends` due to the following reasoning:

- We don't want `diracx-logic`/`diracx-db` to depend on `fastapi`
- `diracx-tasks` shouldn't depend on `diracx-routers`
- Pragmatically, `diracx-tasks` will always be installed alongside `diracx-routers` as it needs to be able to submit tasks. This means we can reuse the same dependency injection system without introducing a new one just for tasks.
- Importing from within the same subpackage hides this implementation detail and allows us to change the implementation in the future without breaking compatibility.

## Rejected Ideas

### Why not a third party library (e.g. Celery, arq, taskiq, dramatiq, ...)?

We evaluated several async Python task queue libraries (Celery, Taskiq, arq, dramatiq). While mature and capable, adapting any of them to DiracX's requirements would require extensive customisation that negates the benefit of using an off-the-shelf solution:

- **Async-native**: DiracX is async-first throughout. Celery and dramatiq are synchronous, immediately ruling them out. Taskiq and arq are async-native but still have the issues below.
- **Entry point discovery**: Libraries assume tasks are defined in application code. We need entry point-based discovery for extensions, requiring a custom task loader.
- **Dependency injection**: DiracX uses FastAPI-style dependency injection for database connections and settings. Libraries have their own DI systems (e.g. Taskiq's `TaskiqDepends`), so we'd maintain two DI containers or build a bridge between them.
- **Declarative locking**: Our `execution_locks()` model — where locks are acquired before execution and configurable per lock type, object type, and even specific object ID — is a task semantic, not a middleware concern. Libraries lack built-in lock primitives and their middleware hooks don't support retry paths for lock acquisition failures.
- **Configuration-driven periodic tasks**: Libraries typically use code-based scheduling (e.g. Celery beat) which doesn't fit our need for configuration-driven periodic tasks that can be enabled/disabled and have their schedules overridden without code changes.
- **Ephemeral broker**: Our durability model discards pending tasks on restart, recreating them from authoritative database state. Task queues treat in-flight tasks as durable, which conflicts with this design and would require working around their persistence guarantees.
- **Priority × size streams**: Nine distinct streams (3 priorities × 3 sizes) for independent worker scaling would require either nine broker instances or heavily customised queue routing.

By building directly on Redis Streams primitives (consumer groups, pending entry lists, sorted sets for scheduling), we avoid the overhead of mapping our requirements onto a general-purpose library's model. The trade-off is that we're Redis-only and must build our own monitoring, but we avoid maintaining a complex adaptation layer where debugging becomes "is this a library issue or our wrapper?".

### Why not persist broker state?

Operating Redis with strong durability guarantees (AOF fsync every write, replication, sentinel failover) adds significant operational complexity. By treating the broker as ephemeral, Redis can be run with without persistence settings since losing its contents is a normal operational event, not a disaster. On restart, startup entry points reset in-flight states in the database (e.g. PENDING → RECEIVED) and the scheduler repopulates the broker from authoritative database state. This also eliminates an entire class of consistency bugs: there is no second source of truth that can diverge from the database (e.g. a task enqueued for a job that has since been cancelled). The trade-off is that the state machine of DiracX objects must be designed to tolerate this reset, but this is a simpler constraint to enforce than guaranteeing broker/database consistency across restarts.

### Make tasks classes aware of resource status (RSS) requirements so status can be enforced by diracx-tasks?

We considered making task classes declare their resource status (RSS) dependencies (e.g. which storage elements or compute elements a task requires) so that diracx-tasks could check resource status (RSS) status before execution and skip tasks targeting banned or degraded resources. This was rejected because tasks often depend on combinations of resource status (RSS) types, a file transfer task may require both a source and destination storage element to be active. Encoding these relationships generically in the task framework would add significant complexity to `BaseTask` for what is ultimately domain logic that varies per task type.

Instead, tasks that interact with resources should check status within their execute() method and raise a retryable exception if a required resource is unavailable. This keeps resource awareness in the domain logic where the specific combination of resources is known. The `RetryPolicyBase.schedule_retry(attempt, exception)` interface already receives the exception, so a retry policy can distinguish between a `ResourceUnavailableError` (retry soon, the resource may recover) and an unrecoverable failure.
