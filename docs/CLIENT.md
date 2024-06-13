## Overview

The DiracX client is a comprehensive toolset designed to interact with various services. It consists of three main components:

```
┌────────┐
│ client │
└────┬───┘
  ┌──▼──┐
  │ api │
  └──┬──┘
  ┌──▼──┐
  │ cli │
  └─────┘
```

1. **diracx-client**: A client library generated from OpenAPI specifications.
2. **diracx-api**: A Python API to interact with services using the diracx-client.
3. **diracx-cli**: A command-line interface for direct interaction with the services.

## diracx-client

The `diracx-client` is an auto-generated client library that facilitates communication with services defined by OpenAPI specifications.

### Client Generation

The client is generated using [AutoRest](https://github.com/Azure/autorest), a tool that reads OpenAPI configurations provided by FastAPI routers.

- **Breaking Changes**: Each time there is a breaking change in a router, the client needs to be regenerated.

### Updating the Client

The CI/CD pipeline handles client regeneration upon each push to the `main` branch. This process helps detect breaking changes in the developer's code, causing the CI/CD to fail if such changes are present.

If a breaking change is acknowledged and approved, developers can manually regenerate the client:

1. Ensure `diracx-client` is installed (refer to the installation documentation).
2. Run the following command to regenerate the client:

```sh
pytest --no-cov --regenerate-client diracx-client/tests/test_regenerate.py
```

### Structure of the Generated Client

The generated client consists of several key components:

- **models**: Represent the data structures.
- **operations**: Contain the methods to interact with the API endpoints.
- **aio**: Asynchronous operations.

Modifications to the generated client should be made in `_patch.py` files to ensure maintainability.

#### Example Usage

Operations are accessible via the `DiracClient`, which manages token refreshment:

```python
from diracx.client.aio import DiracClient

async with DiracClient() as api:
    jobs = await api.jobs.submit_bulk_jobs([x.read() for x in jdl])
```

## diracx-api

The `diracx-api` provides a Python API for interacting with services, leveraging the `diracx-client`.

### API Methods

API methods are located in `diracx-api/src/diracx/api/`. To create an API method:

1. Import `DiracClient`.
2. Decorate the method with `@with_client` to handle client configuration.
3. Pass the `client` as a keyword argument.

#### Example

```python
from diracx.client.aio import DiracClient
from .utils import with_client

@with_client
async def create_sandbox(paths: list[Path], *, client: DiracClient) -> str:
    ...
```

In this example, `paths` are the parameters of the API. The `@with_client` decorator allows the method to be called without manually managing the client:

```python
# Managed by @with_client
# Useful for basic work requiring a single call to the service
result = await create_sandbox(paths)

# For optimized performance with multiple service interactions
async with DiracClient() as api:
    result = await create_sandbox(paths, api)
```

## diracx-cli

The `diracx-cli` is a command-line interface built on `diracx-client` and `diracx-api` for direct interaction with services. It uses [Typer](https://typer.tiangolo.com/) for creating CLI commands and [Rich](https://rich.readthedocs.io/) for enhanced content display.

### CLI Commands

CLI commands are located in `diracx-cli/src/diracx/cli/`. To create a CLI command:

1. Import `DiracClient` and/or the diracx API.
2. Import `utils.AsyncTyper`.
3. Use the `@app.async_command` decorator to define commands.

#### Example

```python
from .utils import AsyncTyper
from diracx.client.aio import DiracClient

app = AsyncTyper()

@app.async_command()
async def submit(jdl: list[FileText]):
    async with DiracClient() as api:
        ...
```

For more details on Typer and Rich options, refer to their [Typer documentation](https://typer.tiangolo.com/) and [Rich documentation](https://rich.readthedocs.io/).

### Associating Commands and Subcommands

- Commands without subcommands (e.g., `dirac login`) should be implemented directly in `src/diracx/__init__.py` and decorated with `app.async_command()`.
- Commands with subcommands (e.g., `dirac jobs submit`) should have their own modules in `src/diracx/<command>` and use `AsyncTyper`.
  - To associate the command with `dirac`, import the module in `src/diracx/__init__.py`:

  ```python
  from . import <command>
  ...

  app.add_typer(<command name>.app, name="<command name>")
  ```

Users can then call the CLI:

```sh
$ dirac <command>
$ dirac <command> <subcommand> [--options]
```
