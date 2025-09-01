# Pixi Tasks

This project uses [pixi](https://pixi.sh/) to manage dependencies and tasks. This page documents the available tasks.

## DiracX Tasks

These tasks are for running tests for the DiracX packages.

- `pytest-diracx`: Runs pytest for the main `diracx` package.
- `pytest-diracx-core`: Runs pytest for the `diracx-core` package.
- `pytest-diracx-cli`: Runs pytest for the `diracx-cli` package.
- `pytest-diracx-api`: Runs pytest for the `diracx-api` package.
- `pytest-diracx-client`: Runs pytest for the `diracx-client` package.
- `pytest-diracx-db`: Runs pytest for the `diracx-db` package.
- `pytest-diracx-logic`: Runs pytest for the `diracx-logic` package.
- `pytest-diracx-routers`: Runs pytest for the `diracx-routers` package.
- `pytest-diracx-all-one-by-one`: Runs all `diracx` pytests one by one.

## Gubbins Tasks

These tasks are for running tests for the Gubbins packages.

- `pytest-gubbins`: Runs pytest for the `gubbins` package.
- `pytest-gubbins-core`: Runs pytest for the `gubbins-core` package.
- `pytest-gubbins-cli`: Runs pytest for the `gubbins-cli` package.
- `pytest-gubbins-api`: Runs pytest for the `gubbins-api` package.
- `pytest-gubbins-client`: Runs pytest for the `gubbins-client` package.
- `pytest-gubbins-db`: Runs pytest for the `gubbins-db` package.
- `pytest-gubbins-logic`: Runs pytest for the `gubbins-logic` package.
- `pytest-gubbins-routers`: Runs pytest for the `gubbins-routers` package.
- `pytest-gubbins-all-one-by-one`: Runs all `gubbins` pytests one by one.

## Documentation Tasks

- `mkdocs`: Serves the documentation locally.
- `mkdocs-build`: Builds the documentation.

## Pre-commit Tasks

- `pre-commit`: Runs pre-commit hooks.

## Client Generation Tasks

- `generate-client`: Generates the client.

## Shellcheck Tasks

- `shellcheck`: Runs shellcheck on all shell scripts.