# Part 4: Router

We add a minimal router with two endpoints for manual interaction
with the pilot system. This lets administrators submit pilots and check
status through the HTTP API, complementing the automated task-based
submission.

## How DiracX discovers routers

DiracX uses entry points to discover routers at startup. The entry point
name determines the URL prefix: a service registered as `my_pilots` is
mounted at `/api/my_pilots/`. This convention means the entry point name
is the single source of truth for URL routing.

## Implementation

<!-- blacken-docs:off -->

```python title="gubbins-routers/src/gubbins/routers/my_pilots.py"
--8<-- "extensions/gubbins/gubbins-routers/src/gubbins/routers/my_pilots.py"
```

<!-- blacken-docs:on -->

### Access policy

`MyPilotsAccessPolicy` controls who can call these endpoints. The
`policy` method is a `@staticmethod` with positional-only args
`(policy_name, user_info)` and keyword-only args with defaults.

!!! note "Why keyword-only args with defaults?"

    This follows the Liskov Substitution Principle — subclasses can add new
    keyword arguments without breaking the base class interface. Every
    policy parameter must have a default value so that the base class can
    call the method without knowing about extension-specific parameters.
    See [Security policies](../../reference/security-policies.md) for the
    full reference.

Here we allow all authenticated users — in a real system you'd check
`user_info` properties to enforce authorization.

### DB dependency

!!! tip "Auto-injected database dependencies"

    Database classes (`BaseSQLDB` subclasses) are auto-detected by
    `DiracxRouter.add_api_route`. Simply type-annotate a parameter with
    the DB class and it will be wrapped with
    `Depends(cls.transaction, scope="function")` automatically, which
    opens a transaction when the request starts and commits on success
    (or rolls back on error).

### Endpoints

- **POST `/submit/{ce_name}`** — Directly inserts a pilot submission
    into the database (bypassing the task system for manual use).
- **GET `/summary`** — Returns pilot counts grouped by status.

For the full guide on building routes, see
[Add a route](../../how-to/add-a-route.md) and the
[Routes explanation](../../explanations/components/routes.md).

## Register entry points

Service entry point:

```toml title="gubbins-routers/pyproject.toml"
--8<-- "extensions/gubbins/gubbins-routers/pyproject.toml:my_pilots_service_entry_point"
```

Access policy entry point:

```toml title="gubbins-routers/pyproject.toml"
--8<-- "extensions/gubbins/gubbins-routers/pyproject.toml:my_pilots_access_policy_entry_point"
```

## Checkpoint

Verify the router works:

```bash
pixi run pytest-gubbins-routers -- -k my_pilots
```
