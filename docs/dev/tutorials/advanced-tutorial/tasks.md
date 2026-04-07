# Part 3: Tasks

Now we implement the four tasks from our design. Each task is a thin
orchestration wrapper that delegates business logic to the
[logic layer](logic.md) we built in the previous part. We'll build
them progressively, from simplest to most complex.

## Custom lock type

Before writing any tasks, we need to register a domain-specific lock
type. Locks prevent concurrent execution of conflicting operations —
for example, two tasks trying to submit a pilot to the same CE
simultaneously.

<!-- blacken-docs:off -->

```python title="gubbins-tasks/src/gubbins/tasks/my_pilot_lock_types.py"
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilot_lock_types.py"
```

<!-- blacken-docs:on -->

`register_locked_object_type` adds `"my_pilot"` to the global lock
registry. This lets you create locks like
`MutexLock(LockedObjectType(MY_PILOT), "some-ce")`, where the first
argument identifies the *type* of thing being locked and the second
identifies *which* instance.

## Imports

The tasks module pulls from several parts of the DiracX task framework.
Start with the imports — we'll reference these as we build each task:

<!-- blacken-docs:off -->

```python title="gubbins-tasks/src/gubbins/tasks/my_pilots.py"
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py:my_pilot_task_imports"
```

<!-- blacken-docs:on -->

The key imports to note:

- **`BaseTask`** — For one-shot tasks triggered on demand
- **`PeriodicBaseTask`** — For tasks that run on a schedule (not
    VO-aware)
- **`PeriodicVoAwareBaseTask`** — For tasks that run per-VO on a
    schedule
- **`Priority` / `Size`** — Task metadata that helps the broker
    allocate resources
- **`NoRetry`** — A retry policy that skips retries entirely
- **`CronSchedule` / `IntervalSeconds`** — Schedule types for periodic
    tasks
- **`gubbins.logic.my_pilots`** — Business logic functions from the
    [logic layer](logic.md). Tasks delegate to these rather than
    implementing logic inline.
- **`MyPilotDB`** (from `gubbins.db.sql`) — The database class from
    Part 2, auto-detected by the dependency injection system

## MyPilotTask — one-shot submission

The simplest task. It receives a CE name and delegates to
`submit_pilot()` from the logic layer, which checks the CE's success
rate and either submits a pilot or raises `PilotSubmissionError`.

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py:my_pilot_task"
```

<!-- blacken-docs:on -->

Several things to notice:

- **Dataclass pattern** — `BaseTask` is a dataclass. Fields like
    `ce_name` become the task's payload, serialised when the task is
    enqueued and deserialised when it executes.
- **Custom locks** — `execution_locks` returns a `MutexLock` keyed on
    the CE name. This means two `MyPilotTask` instances for the *same*
    CE will be serialised, but submissions to *different* CEs can run in
    parallel.
- **`dlq_eligible = False`** — Failed pilot submissions are simply
    discarded. Pilots are ephemeral — the periodic parent will spawn
    replacements on the next cycle. The DLQ is reserved for tasks that
    correspond to external state which must always be recovered (e.g.
    failing to optimise a job).

!!! question "Why `NoRetry` instead of `ExponentialBackoff`?"

    The periodic parent (`MySubmitPilotsTask`) already re-evaluates
    available CEs on every cycle. If a submission fails, the parent will
    discover the slot is still available and spawn a new task. Retrying at
    the child level would duplicate this logic and could cause cascading
    retries under sustained failure.

## MyPilotReportTask — periodic, non-VO-aware

A simple periodic task that logs aggregate statistics across all VOs.

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py:my_pilot_report_task"
```

<!-- blacken-docs:on -->

- **`CronSchedule("0 * * * *")`** — Runs at the top of every hour,
    using standard cron syntax.
- **Non-VO-aware** — Inherits from `PeriodicBaseTask` (not
    `PeriodicVoAwareBaseTask`), so only one instance runs globally.
    This makes sense for aggregate reporting.
- **Default mutex** — With no custom `execution_locks`, the base class
    applies a class-level mutex, ensuring only one report runs at a time.

## MyCheckPilotsTask — VO-aware periodic

This task transitions pilot states. It runs per-VO, so each VO's
pilots are checked independently.

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py:my_check_pilots_task"
```

<!-- blacken-docs:on -->

- **`vo: str` field** — `PeriodicVoAwareBaseTask` is a dataclass with a
    `vo` field. The scheduler creates one instance per configured VO,
    each with its `vo` set automatically.
- **`IntervalSeconds(30)`** — Runs every 30 seconds per VO (as opposed
    to `CronSchedule` which uses wall-clock times).
- **Delegates to logic** — Calls `transition_pilot_states()` which
    moves `SUBMITTED → RUNNING` immediately, then probabilistically
    transitions `RUNNING → DONE` or `RUNNING → FAILED` based on the
    CE's success rate.

## MySubmitPilotsTask — VO-aware periodic, spawns children

The most complex task. It queries for CEs with available capacity and
spawns one-shot `MyPilotTask` instances for each open slot.

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py:my_submit_pilots_task"
```

<!-- blacken-docs:on -->

This demonstrates the **parent-child task pattern** — a periodic task
that dynamically creates one-shot tasks. The parent decides *what* to
do (which CEs need pilots), and each child handles *one* submission.

!!! warning "Broker context required for `schedule()`"

    `await task.schedule()` enqueues the task onto the broker. This only
    works when the current task is running inside a broker context (i.e.,
    executed by the task worker). If you call `schedule()` outside a broker
    context (e.g., in a test or script), it will raise an error. For
    testing, mock the `schedule` method or use the task CLI for direct
    execution.

## Register entry points

Finally, register the tasks and lock type so DiracX discovers them at
startup.

Task entry points go under
`[project.entry-points."diracx.tasks.<group>"]`, where `<group>`
organises related tasks:

```toml title="gubbins-tasks/pyproject.toml"
--8<-- "extensions/gubbins/gubbins-tasks/pyproject.toml:my_pilots_task_entry_points"
```

The entry point names (e.g. `MyPilotTask`) become the task's identifier
in the broker. The dotted path points to the class.

Lock type entry point (add under `[project.entry-points."diracx.lock_object_types"]`):

```toml title="gubbins-tasks/pyproject.toml"
--8<-- "extensions/gubbins/gubbins-tasks/pyproject.toml:my_pilots_lock_entry_point"
```

For more details on the entry point conventions, see
[Add a task](../../how-to/add-a-task.md) and the
[Tasks explanation](../../explanations/tasks/index.md).

## Checkpoint

Verify the tasks are correctly defined:

```bash
pixi run pytest-gubbins-tasks -- -k my_pilot
```
