## diracx-cli

The `diracx-cli` is a command-line interface built on `diracx-client` and `diracx-api` for direct interaction with services. It uses [Typer](https://typer.tiangolo.com/) for creating CLI commands and [Rich](https://rich.readthedocs.io/) for enhanced content display.

### CLI Commands

CLI commands are located in `diracx-cli/src/diracx/cli/`. To create a CLI command:

1. Import `AsyncDiracClient` and/or `diracx-api`.
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

TODO: WRONG

- Commands without subcommands (e.g., `dirac login`) should be implemented directly in `src/diracx/__init__.py` and decorated with `app.async_command()`.

- Commands with subcommands (e.g., `dirac jobs submit`) should have their own modules in `src/diracx/<command>` and use `AsyncTyper`.

    - To associate the command with `dirac`, import the module in `src/diracx/__init__.py`:

    ```python
    from . import command
    ...

    app.add_typer(<command name>.app, name="<command name>")
    ```

Users can then call the CLI:

```sh
$ dirac <command>
$ dirac <command> <subcommand> [--options]
```
