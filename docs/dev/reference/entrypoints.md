# Entrypoints

This page documents the entrypoints used in this project.

## `diracx-cli`

The `diracx-cli` package provides the `diracx` command-line interface.

### `[project.scripts]`

- `dirac = "diracx.cli:app"`: This is the main entry point for the `diracx` command-line interface. It calls the `app` function in `diracx/cli/__init__.py`.

### `[project.entry-points."diracx.cli"]`

These entry points are for the subcommands of the `diracx` command-line interface.

- `jobs = "diracx.cli.jobs:app"`: This is an entry point for the `diracx jobs` subcommand. It calls the `app` function in `diracx/cli/jobs.py`.
- `config = "diracx.cli.config:app"`: This is an entry point for the `diracx config` subcommand. It calls the `app` function in `diracx/cli/config.py`.

### `[project.entry-points."diracx.cli.hidden"]`

These entry points are for hidden subcommands of the `diracx` command-line interface.

- `internal = "diracx.cli.internal:app"`: This is a hidden entry point for the `diracx internal` subcommand. It calls the `app` function in `diracx/cli/internal.py`.

## `gubbins-cli`

The `gubbins-cli` package provides the `gubbins` command-line interface.

### `[project.scripts]`

- `gubbins = "gubbins.cli:app"`: This is the main entry point for the `gubbins` command-line interface. It calls the `app` function in `gubbins/cli/__init__.py`.

### `[project.entry-points."diracx.cli"]`

This entry point extends the `diracx` command with a `security` subcommand.

- `security = "gubbins.cli.security:app"`: This is an entry point for the `diracx security` subcommand. It calls the `app` function in `gubbins/cli/security.py`.

## Extending the `diracx` command

The `diracx.cli` entrypoint group allows extending the `diracx` command with subcommands from other packages. This is used by the `gubbins-cli` package to add the `security` subcommand to the `diracx` command.
