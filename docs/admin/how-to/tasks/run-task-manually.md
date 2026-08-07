# Run a task manually

The `diracx-task-run call` command executes a single task interactively, bypassing the broker. This is useful for debugging, manual recovery, and verifying task behaviour.

## Basic usage

```bash
diracx-task-run call <entry_point> --args '<JSON list>'
```

The entry point name is `<category>:<ClassName>` as registered in `pyproject.toml`. For example:

```bash
diracx-task-run call lollygag:SyncOwnersTask --args '["alice"]'
```

## Passing arguments

- **`--args`**: JSON list of positional arguments passed to the task constructor (default: `[]`)
- **`--kwargs`**: JSON dict of keyword arguments (default: `{}`)

```bash
diracx-task-run call lollygag:SyncOwnersTask --args '["alice"]' --kwargs '{}'
```

## Run the dummy job executor

The task package includes two tasks that simulate job execution. The periodic
`jobs:DummyJobExecutorMonitorTask` moves every `Received` job to `Waiting` and
schedules a one-shot `jobs:DummyJobExecutorTask` for it, which walks the job
through `Matched` → `Running` → `Done`.

Automatic monitoring is disabled by default. `run_local.sh` explicitly enables
the monitor every 10 seconds with
`DIRACX_TASKS_DUMMY_JOB_EXECUTOR_ENABLED=true` and
`DIRACX_TASKS_DUMMY_JOB_EXECUTOR_INTERVAL_SECONDS=10`. Demo deployment
enablement requires the coordinated `diracx-charts` values change and is not
part of this commit.

Both tasks talk to the job databases, so the relevant `DIRACX_DB_URL_*`,
`DIRACX_OS_DB_*`, and `DIRACX_CONFIG_BACKEND_URL` variables must be set.

Run the monitor once to pick up all `Received` jobs:

```bash
diracx-task-run call jobs:DummyJobExecutorMonitorTask
```

Or simulate the execution of a single job by passing its job ID:

```bash
diracx-task-run call jobs:DummyJobExecutorTask --args '[42]'
```

## Debugging

The `--debugger` flag drops into Python's debugger:

- **`--debugger before`**: break before task execution
- **`--debugger exception`**: break on unhandled exception (post-mortem)

```bash
diracx-task-run call lollygag:SyncOwnersTask --args '["alice"]' --debugger exception
```

## Lock behaviour in interactive mode

When `DIRACX_TASKS_REDIS_URL` is set, structural locks (`MutexLock`, `ExclusiveRWLock`, `SharedRWLock`) are still acquired — this prevents accidentally corrupting shared state. Limiters (`RateLimiter`, `ConcurrencyLimiter`) are skipped, so the task runs without being throttled.

If `DIRACX_TASKS_REDIS_URL` is not set, no locks are acquired at all.

## Listing available tasks

To see which tasks are registered, run the command with an invalid entry point name:

```bash
diracx-task-run call nonexistent
# Task 'nonexistent' not found. Available: ['lollygag:OwnerCleanupTask', 'lollygag:OwnerReportTask', 'lollygag:SyncOwnersTask']
```
