# Part 2: Database

We create a new `MyPilotDB` class from scratch (not extending an
existing DB). This shows the full lifecycle of adding a database to
DiracX — from schema definition through to dependency injection.

## Schema

The schema defines our two tables and the status enum:

<!-- blacken-docs:off -->

```python title="gubbins-db/src/gubbins/db/sql/my_pilot_db/schema.py"
--8<-- "extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db/schema.py"
```

<!-- blacken-docs:on -->

Let's unpack the DiracX database conventions used here:

- **`DeclarativeBase`** — Each database module defines its own `Base`
    subclass. This keeps table metadata isolated so that different
    databases don't interfere with each other.
- **`str255`** — A DiracX type alias that maps to `String(255)` via
    `type_annotation_map`. Use it for any short text column.
- **`datetime_now`** — Provides a server-default UTC timestamp, so you
    don't need to pass timestamps explicitly on insert.
- **`metadata`** — The `Base.metadata` object tracks all tables
    belonging to this database. You'll pass it to `BaseSQLDB` in the
    next step.

!!! note "The `type_annotation_map` pattern"

    The `type_annotation_map` on `Base` tells SQLAlchemy how to translate
    Python type annotations into SQL column types. When you write
    `name: Mapped[str255]`, SQLAlchemy looks up `str255` in this map and
    uses `String(255)`. This keeps column type information in one place
    rather than repeating `type_=String(255)` on every column.

!!! question "Why `StrEnum` instead of a SQLAlchemy `Enum` column?"

    We store status as a plain string rather than using a SQL `ENUM` type.
    This makes the schema portable across database backends (SQLite doesn't
    support `ENUM`) and avoids costly `ALTER TABLE` commands when adding new
    statuses. The `StrEnum` on the Python side still gives you
    autocompletion and validation.

## DB class

The DB class wraps SQLAlchemy queries behind a clean async interface:

<!-- blacken-docs:off -->

```python title="gubbins-db/src/gubbins/db/sql/my_pilot_db/db.py"
--8<-- "extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db/db.py"
```

<!-- blacken-docs:on -->

`BaseSQLDB` gives you several things out of the box:

- **`self.conn`** — An async database connection, scoped to the current
    transaction
- **Transaction lifecycle** — Transactions are opened when you enter
    `async with db:` and committed (or rolled back) on exit
- **`metadata`** — Links the DB class to its schema so DiracX can
    auto-create tables

!!! tip "The subquery + coalesce pattern in `get_available_ces()`"

    The `get_available_ces()` method is the most interesting query here. It
    uses a subquery to count active pilots per CE, then outer-joins this to
    the CE table. The `func.coalesce(active_counts.c.active, 0)` handles
    CEs with no active pilots (where the outer join produces `NULL`). This
    is a common SQLAlchemy pattern for "count related rows and filter by the
    result".

For a deeper understanding of how transactions work, see the
[DB transaction model](../../reference/db-transaction-model.md)
reference. The [Databases explanation](../../explanations/components/db.md)
covers the broader architecture.

## Create the `__init__.py`

Create an empty `my_pilot_db/__init__.py` file in the same directory.

## Register the entry point and export

The next two steps connect your database to the rest of DiracX.

!!! note "Why two registration steps?"

    1. **Entry point** (in `pyproject.toml`) — Tells DiracX's plugin
        system that this DB exists. The entry point name becomes the DB's
        identifier in configuration and connection URLs.
    2. **Package export** (in `__init__.py`) — Makes the DB class
        importable from the top-level package so other code (routers,
        tasks) can reference it.

    Dependency injection is handled automatically — `auto_inject_depends`
    detects `BaseSQLDB` subclasses and wraps them with the appropriate
    `Depends` annotation. No manual `Annotated` wrapper is needed.

    See [Entrypoints](../../reference/entrypoints.md) and
    [Dependency injection](../../reference/dependency-injection.md)
    for the full picture.

### Entry point

Add under `[project.entry-points."diracx.dbs.sql"]`:

```toml title="gubbins-db/pyproject.toml"
--8<-- "extensions/gubbins/gubbins-db/pyproject.toml:my_pilots_db_entry_point"
```

### Package export

<!-- blacken-docs:off -->

```python title="gubbins-db/src/gubbins/db/sql/__init__.py"
--8<-- "extensions/gubbins/gubbins-db/src/gubbins/db/sql/__init__.py:my_pilots_db_init"
```

<!-- blacken-docs:on -->

### Helm chart

For deployed environments (including CI), the database must also be
listed in the extension's Helm chart values. This tells the
infrastructure to create the database and set the
`DIRACX_DB_URL_MYPILOTDB` environment variable that the application
reads at startup.

Add under `diracx.diracx.sqlDbs.dbs` in your chart's `values.yaml`:

```yaml title="gubbins-charts/values.yaml"
--8<-- "extensions/gubbins-charts/values.yaml:my_pilots_helm_db"
```

!!! note "Local dev doesn't need this"

    `pixi run local-start` uses `generate-local-urls` which
    auto-discovers databases from entry points. The Helm chart step is
    only required for Kubernetes-based deployments.

## Checkpoint

At this point, verify the database layer works before moving on:

```bash
pixi run pytest-gubbins-db -- -k my_pilot
```
