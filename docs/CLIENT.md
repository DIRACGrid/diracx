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

The `diracx-client` consists of three parts:
* an auto-generated client library that facilitates communication with services defined by OpenAPI specifications. (the `generated` folder)
* customization, in the `patches` folder, which mirror the structure of the generated client.
* the base modules (`aio`, `extensions`, `models`) just exporting what we want to be exporting

`diracx-client` also defines a `AsyncDiracClient` class which exposes all these low level calls, and handles the authentication/authorisation aspects, as well as the interactions with extensions.

### Generating a Client

The client is generated using [AutoRest](https://github.com/Azure/autorest), a tool that reads OpenAPI configurations provided by FastAPI routers.

- **Breaking Changes**: Each time there is a breaking change in a router, the client needs to be regenerated.

### Updating the Client

The CI/CD pipeline handles client regeneration upon each push to the `main` branch. This process helps detect breaking changes in the developer's code, causing the CI/CD to fail if such changes are present.

If a breaking change is acknowledged and approved, one of the repo admin will regenerate the client on behalf of the developer. Developers can still manually regenerate the client but it requires a few additional tools. The best up-to-date documentation lies in the [`client-generation` CI job](https://github.com/DIRACGrid/diracx/blob/main/.github/workflows/main.yml).

### Structure of the Generated Client

The generated client consists of several key components:

- **models**: Represent the data structures.
- **operations**: Contain the methods to interact with the API endpoints.
- **aio**: Asynchronous client.

Further details can be found in the [Python Autorest documentation](https://github.com/Azure/autorest.python/blob/main/docs/client/readme.md).

### Customising the Generated Client

Modifications to the generated client should be made in the `patches` files to ensure maintainability, and possibly imported in the `_patch.py` files if needed. Detailed guidance can be found in [Python Autorest documentation](https://github.com/Azure/autorest.python/blob/main/docs/customizations.md).

Note: any modification in the synchronous client should also be performed in the asynchronous client (**aio**), and vice-versa.

#### Example Usage

Operations are accessible via the `AsyncDiracClient`, which manages token refreshment:

```python
from diracx.client.aio import AsyncDiracClient

async with AsyncDiracClient() as client:
    jobs = await client.jobs.submit_jobs([x.read() for x in jdl])
```

### Configuring the Generated Client

Clients need to be configured to interact with services. This is performed through **DiracxPreferences**, which is a [BaseSettings Pydantic model](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) that load configuration from the environment.

### Configuring a client

Required environment variables to interact with the services:

- `DIRACX_URL`: the URL pointing to diracx services
- `DIRACX_CA_PATH`: CA path used by the diracx services

Optional environment variables:

- `DIRACX_OUTPUT_FORMAT`: output format (e.g. `JSON`). Default value depends whether the output stream is associated to a terminal.
- `DIRACX_LOG_LEVEL`: logging level (e.g. `ERROR`). Defaults to `INFO`.
- `DIRACX_CREDENTIALS_PATH`: path where access and refresh tokens are stored. Defaults to `~/.cache/diracx/credentials.json`.


### Getting preferences

Developers can get access to the preferences through the following method:

```python
from diracx.core.preferences import get_diracx_preferences

...

credentials_path = get_diracx_preferences().credentials_path
```

Note: preferences are cached.

## diracx-api

The `diracx-api` provides a Python API for interacting with services, leveraging the `diracx-client`.

### API Methods

API methods are located in `diracx-api/src/diracx/api/`. To create an API method:

1. Import `AsyncDiracClient`.
2. Decorate the method with `@with_client` to handle client configuration.
3. Pass the `client` as a keyword argument.

#### Example

```python
from diracx.client.aio import AsyncDiracClient
from .utils import with_client

@with_client
async def create_sandbox(paths: list[Path], *, client: AsyncDiracClient) -> str:
    ...
```

In this example, `paths` are the parameters of the API. The `@with_client` decorator allows the method to be called without manually managing the client:

```python
# Managed by @with_client
# Useful for basic work requiring a single call to the service
result = await create_sandbox(paths)

# For optimised performance with multiple service interactions
async with AsyncDiracClient() as client:
    result = await create_sandbox(paths, client)
```

## diracx-cli

The `diracx-cli` is a command-line interface built on `diracx-client` and `diracx-api` for direct interaction with services. It uses [Typer](https://typer.tiangolo.com/) for creating CLI commands and [Rich](https://rich.readthedocs.io/) for enhanced content display.

### CLI Commands

CLI commands are located in `diracx-cli/src/diracx/cli/`. To create a CLI command:

1. Import `AsyncDiracClient` and/or the diracx API.
2. Import `utils.AsyncTyper`.
3. Use the `@app.async_command` decorator to define commands.

For adding a new command, it needs to be added to one of the following entrypoint:

```toml
[project.entry-points."diracx.cli"]
jobs = "diracx.cli.jobs:app"
config = "diracx.cli.config:app"

[project.entry-points."diracx.cli.hidden"]
internal = "diracx.cli.internal:app"
```

#### Example

```python
from .utils import AsyncTyper
from diracx.client.aio import AsyncDiracClient

app = AsyncTyper()

@app.async_command()
async def submit(jdl: list[FileText]):
    async with AsyncDiracClient() as client:
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
