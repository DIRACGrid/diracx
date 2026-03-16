# Part 7: Testing the router

Router tests use DiracX's test client infrastructure to spin up an
in-memory server with real dependency injection — no external services
needed.

## Full test file

<!-- blacken-docs:off -->

```python title="gubbins-routers/tests/test_my_pilots.py"
--8<-- "extensions/gubbins/gubbins-routers/tests/test_my_pilots.py"
```

<!-- blacken-docs:on -->

## Test infrastructure

The `pytestmark` line at the top configures which dependencies the test
client should wire up:

```python
pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "MyPilotDB",
        "MyPilotsAccessPolicy",
        "DevelopmentSettings",
    ]
)
```

Each entry serves a specific purpose:

- **`AuthSettings`** — Provides token generation and validation so the
    test client can authenticate requests.
- **`MyPilotDB`** — Spins up an in-memory SQLite database and injects
    it into the router endpoints (the same pattern as the database
    tests, but handled automatically).
- **`MyPilotsAccessPolicy`** — Registers the access policy so the
    router can authorize requests.
- **`DevelopmentSettings`** — Provides default configuration suitable
    for testing.

## The `client_factory` fixture

`client_factory` is provided by DiracX's test infrastructure. Calling
`client_factory.normal_user()` returns an HTTP client that is
pre-authenticated with a valid token — so you can focus on testing
endpoint logic rather than dealing with auth setup.

```python
@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client
```

The `with` block ensures the client is properly cleaned up after the
test.

## The test

`test_pilot_summary_empty` verifies the simplest case: with no data in
the database, the summary endpoint returns `200 OK` with an empty dict.

```python
def test_pilot_summary_empty(normal_user_client):
    r = normal_user_client.get("/api/my_pilots/summary")
    assert r.status_code == 200
    assert r.json() == {}
```

The URL prefix `/api/my_pilots/` comes from the entry point name
`my_pilots` registered in Part 4. Testing the empty case is valuable
because it verifies the full stack is wired correctly — dependency
injection, database creation, access policy, and serialization — without
needing any test data.

For more testing patterns, see
[Writing tests](../../reference/writing-tests.md) and
[Test recipes](../../reference/test-recipes.md).

## Run the tests

```bash
pixi run pytest-gubbins-routers -- -k my_pilots
```

## Final checkpoint

Run all tutorial tests to verify everything works together:

```bash
pixi run test-tutorial
```
