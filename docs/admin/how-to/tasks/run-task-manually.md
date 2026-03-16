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
