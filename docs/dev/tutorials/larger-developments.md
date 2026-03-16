# Larger developments

The [Getting started](getting-started.md) tutorial showed how to make a simple CLI change in a single package.
Most real work touches multiple packages — a new feature might need a database table, business logic, an HTTP endpoint, and tests.
This tutorial orients you within the codebase so you know where each piece lives and how to work across packages.

## The package structure

DiracX is split into [namespace packages](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/) with a clear dependency chain:

```
diracx-core → diracx-db → diracx-logic → diracx-routers
                                        → diracx-tasks
```

Each package has a distinct responsibility:

| Package          | Role                                               | Example                               |
| ---------------- | -------------------------------------------------- | ------------------------------------- |
| `diracx-core`    | Domain models, settings, shared types              | `JobStatus` enum, Pydantic models     |
| `diracx-db`      | Database tables and data-access methods            | SQLAlchemy table definitions, queries |
| `diracx-logic`   | Business logic that orchestrates DB calls          | "submit this job" logic               |
| `diracx-routers` | HTTP API (FastAPI)                                 | `POST /api/jobs/` endpoint            |
| `diracx-tasks`   | Background work (periodic or on-demand)            | Cleaning up stale jobs                |
| `diracx-client`  | Auto-generated Python client from the OpenAPI spec | `DiracClient().jobs.submit()`         |
| `diracx-api`     | Higher-level operations on top of the client       | Convenience wrappers                  |
| `diracx-cli`     | The `dirac` command-line tool                      | `dirac jobs submit`                   |

The source for each lives in a top-level directory:

```
diracx/
├── diracx-core/src/diracx/core/
├── diracx-db/src/diracx/db/
├── diracx-logic/src/diracx/logic/
├── diracx-routers/src/diracx/routers/
├── diracx-tasks/src/diracx/tasks/
├── diracx-client/src/diracx/client/
├── diracx-api/src/diracx/api/
├── diracx-cli/src/diracx/cli/
└── extensions/
    └── gubbins/          # reference extension
```

When you run `pixi install`, all packages are installed in editable mode — any source change is immediately available without reinstalling.

For a more detailed overview of the architecture, see the [components explanation](../explanations/components/index.md) and [repository structure](../explanations/repo-structure.md).

## Where does my code go?

When starting a new feature, ask:

1. **Am I adding to core DiracX or an extension?** Core changes go in the `diracx-*` packages. Community-specific features go in an extension (see [below](#the-gubbins-extension)).
2. **Which packages will I touch?** Follow the dependency chain. A new HTTP endpoint typically means changes in `diracx-db` + `diracx-logic` + `diracx-routers` at minimum.

To build intuition, trace an existing feature across the codebase. For example, to understand how jobs work, you could follow the chain:

- **CLI** → `diracx-cli/src/diracx/cli/jobs.py` — the `dirac jobs submit` command
- **Client** → `diracx-client/src/diracx/client/` — the auto-generated client that calls the API
- **Router** → `diracx-routers/src/diracx/routers/jobs/` — the FastAPI endpoint
- **Logic** → `diracx-logic/src/diracx/logic/jobs/` — the business logic
- **DB** → `diracx-db/src/diracx/db/sql/jobs/` — the database tables and queries

Each layer has a clear responsibility. The how-to guides cover adding each component:

- [Add a route](../how-to/add-a-route.md)
- [Add a DB](../how-to/add-a-db.md)
- [Add a task](../how-to/add-a-task.md)

## The gubbins extension

[`gubbins`](../explanations/extensions.md) is the reference DiracX extension, bundled in the repository under `extensions/gubbins/`.
It mirrors the structure of the core packages:

```
extensions/gubbins/
├── gubbins-core/
├── gubbins-db/
├── gubbins-logic/
├── gubbins-routers/
├── gubbins-tasks/
├── gubbins-client/
├── gubbins-api/
├── gubbins-cli/
└── gubbins-testing/
```

Gubbins serves two purposes:

1. **Reference implementation** — it demonstrates every supported extension pattern. Any use case not shown in gubbins is not officially supported.
2. **Tutorial playground** — the [Advanced tutorial](advanced-tutorial/index.md) builds its code inside gubbins.

If you're building your own extension, use gubbins as a template: reproduce its directory layout, `pyproject.toml` structure, and entry points, replacing `gubbins` with your extension name.

For more details, see the [extensions explanation](../explanations/extensions.md).

## Running and testing your changes

### Targeted tests

Each package has its own test suite. Run only the tests relevant to the package you're working on:

```bash
pixi run pytest-diracx-routers   # tests for diracx-routers
pixi run pytest-diracx-logic     # tests for diracx-logic
pixi run pytest-gubbins-db       # tests for gubbins-db
```

See the full list of available test tasks in the [pixi tasks reference](../reference/pixi-tasks.md).

To run the full test suite for all core packages:

```bash
pixi run pytest-diracx
```

Or for the gubbins extension:

```bash
pixi run pytest-gubbins
```

### Running locally

For development that goes beyond unit tests — for example, manually testing an API endpoint or experimenting with the task scheduler — you can run the full DiracX stack locally:

```bash
pixi run local-start
```

This launches everything you need (SeaweedFS, Redis, uvicorn, the scheduler, and workers) as local processes without Docker.
It's the recommended way to develop and debug interactively.

!!! note "local-start vs. the demo"

    `pixi run local-start` runs DiracX as local processes — fast to start, easy to debug, and sufficient for most backend development.

    The [Run a full DiracX instance locally](run-locally.md) tutorial covers the heavier Docker/Kubernetes-based **demo** (via `diracx-charts`), which is useful when you need the web UI, legacy DIRAC integration, or a production-like environment.

## Next steps

Now that you understand how the pieces fit together:

- **Build something from scratch** — the [Advanced tutorial](advanced-tutorial/index.md) walks through creating a complete subsystem (database, tasks, router, tests) inside gubbins.
- **Look up specific tasks** — the [how-to guides](../how-to/index.md) cover individual operations like adding a route, a database, or a task.
- **Understand the design** — the [explanations](../explanations/index.md) go deeper into architecture decisions and component design.
