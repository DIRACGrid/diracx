# Part 2b: Logic

Business logic lives in the **logic layer**, separate from both the
database (which handles persistence) and the tasks (which handle
orchestration and scheduling). This separation means the same logic
can be called from tasks, routers, or tests without coupling to any
specific execution context.

## Why a separate logic layer?

DiracX has three layers that can contain "code that does things":
routers, tasks, and logic. Each has a distinct role:

| Layer      | Owns                                                                   | Should NOT contain             |
| ---------- | ---------------------------------------------------------------------- | ------------------------------ |
| **Router** | HTTP concerns: authentication, serialisation, request/response mapping | Domain rules, DB orchestration |
| **Task**   | Scheduling, retries, locks, child spawning                             | Domain rules, DB orchestration |
| **Logic**  | Domain rules: validations, state machines, multi-step DB orchestration | HTTP or scheduling concerns    |

The key insight is that **the same business operation often needs to
run from multiple entry points**. For example, submitting a pilot
might be triggered by a periodic task *and* by an HTTP endpoint for
manual use. If the submission logic lives inside a task's `execute`
method, the router must either duplicate it or awkwardly call the task
synchronously. Putting it in the logic layer makes it callable from
both without coupling.

In practice, task `execute` methods should be thin orchestration
wrappers — they know *when* and *how often* to run, but delegate the
*what* to the logic layer. Similarly, router endpoints handle HTTP
concerns and delegate to logic for the actual work.

## Implementation

Create the pilot business logic module:

<!-- blacken-docs:off -->

```python title="gubbins-logic/src/gubbins/logic/my_pilots.py"
--8<-- "extensions/gubbins/gubbins-logic/src/gubbins/logic/my_pilots.py"
```

<!-- blacken-docs:on -->

Key points:

- **Pure async functions** — Each function takes a database instance
    and any required parameters. No framework dependencies, no
    scheduling concerns.
- **Custom exception** — `PilotSubmissionError` replaces a generic
    `RuntimeError`, making error handling more precise in callers.
- **DB type hints** — Functions use the raw `MyPilotDB` class directly.
    The dependency injection system auto-detects DB classes at runtime.

## Update dependencies

Add `gubbins-db` to `gubbins-logic`'s dependencies so the import of
`MyPilotDB` resolves:

```toml title="gubbins-logic/pyproject.toml" hl_lines="3"
dependencies = [
    "diracx-logic",
    "gubbins-db",
]
```

## Checkpoint

At this point the logic module is a standalone library with no task or
router dependencies. In the next part we'll wire it into the task
layer.
