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

## Run the Condor job executor

The task package includes two tasks for handing jobs to HTCondor. The periodic
`jobs:CondorJobExecutorMonitorTask` moves every `Received` job to `Waiting` and
schedules a one-shot `jobs:CondorJobExecutorTask` for it. The executor task is
the integration point where HTCondor submission should be implemented.

Automatic monitoring is disabled by default and controlled via
`DIRACX_TASKS_CONDOR_JOB_EXECUTOR_ENABLED` and
`DIRACX_TASKS_CONDOR_JOB_EXECUTOR_INTERVAL_SECONDS`.

Run the monitor once to pick up all `Received` jobs:

```bash
diracx-task-run call jobs:CondorJobExecutorMonitorTask
```

Or invoke the one-shot executor for a specific job ID:

```bash
diracx-task-run call jobs:CondorJobExecutorTask --args '[42]'
```

## Passing arguments

- **`--args`**: JSON list of positional arguments passed to the task constructor (default: `[]`)
- **`--kwargs`**: JSON dict of keyword arguments (default: `{}`)

```bash
diracx-task-run call lollygag:SyncOwnersTask --args '["alice"]' --kwargs '{}'
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
