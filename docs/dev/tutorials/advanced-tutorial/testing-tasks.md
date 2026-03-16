# Part 6: Testing the tasks

Task tests verify the logic, metadata, and locking behaviour of each
task type. The database is mocked — its own tests (Part 5) cover the
query layer, so here we focus on what the task *does* with its
dependencies.

## Full test file

<!-- blacken-docs:off -->

```python title="gubbins-tasks/tests/test_my_pilot_tasks.py"
--8<-- "extensions/gubbins/gubbins-tasks/tests/test_my_pilot_tasks.py"
```

<!-- blacken-docs:on -->

!!! note "Why mock the database?"

    Task tests use `AsyncMock()` for the database instead of a real
    connection. This is a deliberate choice: the database layer has its own
    tests (Part 5), so task tests focus purely on the task's logic —
    branching, error handling, and which DB methods get called. This keeps
    the tests fast and the failure messages precise.

## Lock type registration

`test_my_pilot_lock_type_registered` verifies that the `MY_PILOT`
constant was registered via `register_locked_object_type` and can be
used to construct a `LockedObjectType`. If the entry point is missing
or the string doesn't match, this test catches it immediately.

## MyPilotTask — one-shot submission

Six tests cover the one-shot task progressively:

### Serialization and properties

`test_my_pilot_task_serialize` checks that the task round-trips through
`serialize()` — this is what the broker stores when the task is
enqueued.

`test_my_pilot_task_properties` verifies the class-level metadata:
`Priority.NORMAL`, `Size.SMALL`, `NoRetry` policy, and
`dlq_eligible=False`. These control how the broker prioritises,
routes, and handles failures for this task.

### Locks

`test_my_pilot_task_locks` verifies that a task for
`"reliable-ce.example.org"` produces a single `MutexLock` whose
`redis_key` contains both `"my_pilot"` (the lock type) and the CE
name. This is how the broker prevents two submissions to the same CE
from running simultaneously.

`test_my_pilot_task_different_ces_different_locks` confirms that
different CE names produce different lock keys — so submissions to
*different* CEs can run in parallel.

### Execution

`test_my_pilot_task_execute_success` wires up an `AsyncMock` database
with `success_rate=1.0` and checks that `execute()` calls
`submit_pilot` and returns the pilot ID.

`test_my_pilot_task_execute_failure` uses `success_rate=0.0` — the
random check always fails, so `execute()` should raise `RuntimeError`
and never call `submit_pilot`.

!!! tip "Deterministic randomness"

    The tests use `success_rate=1.0` and `success_rate=0.0` to make
    `random.random()` comparisons deterministic. With `success_rate=1.0`,
    `random.random() >= 1.0` is always `False` (success). With
    `success_rate=0.0`, it's always `True` (failure). No need to mock
    `random` — the math does the work.

## MyPilotReportTask — periodic report

Three tests cover the non-VO-aware periodic task:

- `test_my_pilot_report_task_schedule` — Verifies the cron expression
    is `"0 * * * *"` (top of every hour).
- `test_my_pilot_report_task_locks` — Checks the default class-level
    mutex lock (one report at a time globally).
- `test_my_pilot_report_task_execute` — Mocks the DB, calls
    `execute()`, and asserts `get_pilot_summary` was called and the
    result passed through.

## MyCheckPilotsTask — VO-aware periodic

Four tests for the VO-aware state-transition task:

- `test_my_check_pilots_task_schedule` — Interval is 30 seconds.
- `test_my_check_pilots_task_serialize` — `vo="lhcb"` serializes to
    `("lhcb",)`.
- `test_my_check_pilots_task_locks_include_vo` — The lock key includes
    the VO name, so `lhcb` and `atlas` can run concurrently.
- `test_my_check_pilots_task_different_vos_different_locks` — Confirms
    different VOs produce different lock keys.

## MySubmitPilotsTask — VO-aware periodic, spawns children

Three tests mirror the pattern above:

- `test_my_submit_pilots_task_schedule` — Interval is 60 seconds.
- `test_my_submit_pilots_task_serialize` — `vo="lhcb"` serializes
    correctly.
- `test_my_submit_pilots_task_locks_include_vo` — Lock key includes
    the VO name.

These tests verify the task *definition* — the execution logic
(spawning `MyPilotTask` children) requires a broker context and is
tested at the integration level.

## Run the tests

```bash
pixi run pytest-gubbins-tasks -- -k my_pilot
```
