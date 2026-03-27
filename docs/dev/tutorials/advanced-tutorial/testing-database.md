# Part 5: Testing the database

Database tests verify your schema and queries in isolation using an
in-memory SQLite backend. Each test gets a fresh database, so there's no
shared state between tests.

## Full test file

<!-- blacken-docs:off -->

```python title="gubbins-db/tests/test_my_pilot_db.py"
--8<-- "extensions/gubbins/gubbins-db/tests/test_my_pilot_db.py"
```

<!-- blacken-docs:on -->

## The fixture

The `my_pilot_db` fixture creates a throwaway database for each test:

- **In-memory SQLite** —
    `MyPilotDB("sqlite+aiosqlite:///:memory:")` avoids touching the
    filesystem. Each test starts clean.
- **Engine lifecycle** — `engine_context()` manages the async engine.
    Inside it, `engine.begin()` opens a connection and
    `metadata.create_all` creates the tables from your schema.
- **`yield`** — The fixture yields the DB instance to the test. When the
    test finishes, the context manager tears down the engine.

This pattern is common across DiracX database tests — you'll see the
same structure whenever a test needs a real database connection.

## Basic CRUD

`test_add_and_get_ces` is the simplest test: insert two CEs, then query
them back. The two-`async with db:` pattern is important — the insert
happens in one transaction, the query in another. This verifies that
data was actually committed, not just visible within the same
transaction.

`test_disabled_ce_not_available` checks that `enabled=False` excludes a
CE from `get_available_ces()`. This tests the filter clause in the
query without needing to set up pilots or capacity.

!!! tip "Each `async with db:` is a separate transaction"

    This mirrors how production code works: each HTTP request or task
    execution gets its own transaction. If you insert and query in the
    same `async with db:` block, you're only testing that the data is
    visible within a single transaction — not that it was committed.

## Pilot lifecycle

`test_submit_pilot` verifies the insert-and-query flow: add a CE,
submit a pilot, then check that `get_pilots_by_status(SUBMITTED)`
returns it. Three separate transactions ensure each operation commits
independently.

`test_update_pilot_status` tests status transitions: submit a pilot,
move it to `RUNNING`, then verify that querying `SUBMITTED` returns
nothing and `RUNNING` returns one result. This exercises the `UPDATE`
query and confirms the status column is actually changing.

## Capacity tracking

`test_capacity_tracking` is the most interesting test. It exercises the
subquery from `get_available_ces()` end-to-end:

1. Add a CE with `capacity=2`
2. Submit 2 pilots — CE should no longer appear in
    `get_available_ces()` (capacity full)
3. Complete one pilot (`DONE`) — the CE reappears with
    `available_slots == 1`

This verifies the coalesce/subquery pattern that counts active pilots
and subtracts them from capacity. It also confirms that terminal states
(`DONE`, `FAILED`) don't count against capacity.

## Aggregates

`test_get_ce_success_rate` is a simple lookup — add a CE with
`success_rate=0.75`, then verify it reads back correctly.

`test_pilot_summary` tests the grouping query: submit three pilots,
transition one to `RUNNING`, then check that the summary returns
`{SUBMITTED: 2, RUNNING: 1}`. This exercises `GROUP BY` with the
status enum.

## Run the tests

```bash
pixi run pytest-gubbins-db -- -k my_pilot
```
