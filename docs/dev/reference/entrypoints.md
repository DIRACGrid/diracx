# DiracX Entry Points Reference

This document catalogs all available entry points for creating DiracX extensions.
Entry points are defined in `pyproject.toml` files and discovered at runtime.

## Table of Contents

- [Core Extension Registration](#diracx)
- [Access Policy Registration](#diracxaccess-policies)
- [CLI Command Registration](#diracxcli)
- [Hidden CLI Commands](#diracxclihidden)
- [OpenSearch Database Registration](#diracxdbsos)
- [SQL Database Registration](#diracxdbssql)
- [Minimum Client Version Declaration](#diracxmin-client-version)
- [Resource Management Functions](#diracxresources)
- [FastAPI Router Registration](#diracxservices)

## Core Extension Registration

**Entry Point Group**: `diracx`

The base entry point group for registering DiracX extensions. Extensions MUST register themselves here.

### Entry Point Keys

- **`extension`**: Extension name (required for all extensions)
- **`properties_module`**: Module path to custom DIRAC properties
- **`config`**: Path to extended configuration schema class

### Usage Example

```toml
[project.entry-points."diracx"]
extension = "myextension"
properties_module = "myextension.core.properties"
config = "myextension.core.config.schema:Config"
```

### Important Notes

- The `extension` key is **required** for all extensions
- Extensions are prioritized by name (alphabetically, with 'diracx' last)
- Only one extension can be installed alongside DiracX core

### Current Implementations

| Package          | Entry Name          | Entry Point                                  |
| ---------------- | ------------------- | -------------------------------------------- |
| `diracx-core`    | `config`            | `diracx.core.config.schema:Config`           |
| `diracx-core`    | `extension`         | `diracx`                                     |
| `diracx-core`    | `properties_module` | `diracx.core.properties`                     |
| `gubbins-client` | `aio_client_class`  | `gubbins.client.generated.aio._client:Dirac` |
| `gubbins-client` | `client_class`      | `gubbins.client.generated._client:Dirac`     |
| `gubbins-core`   | `config`            | `gubbins.core.config.schema:Config`          |
| `gubbins-core`   | `extension`         | `gubbins`                                    |
| `gubbins-core`   | `properties_module` | `gubbins.core.properties`                    |

## Access Policy Registration

**Entry Point Group**: `diracx.access_policies`

Register custom access policies for fine-grained authorization control. Policies can inject claims into tokens and check permissions at runtime.

### Entry Point Keys

- **`<PolicyName>`**: Path to BaseAccessPolicy subclass

### Usage Example

```toml
[project.entry-points."diracx.access_policies"]
WMSAccessPolicy = "myextension.routers.jobs.access_policy:WMSAccessPolicy"
CustomPolicy = "myextension.routers.custom.policy:CustomAccessPolicy"
```

### Important Notes

- Policies must inherit from `BaseAccessPolicy`
- Each route must call its policy or use `@open_access` decorator
- Policies can inject data during token generation via `policy_name` claim
- CI test `test_all_routes_have_policy` enforces policy usage

### Current Implementations

| Package           | Entry Name             | Entry Point                                                   |
| ----------------- | ---------------------- | ------------------------------------------------------------- |
| `diracx-routers`  | `SandboxAccessPolicy`  | `diracx.routers.jobs.access_policies:SandboxAccessPolicy`     |
| `diracx-routers`  | `WMSAccessPolicy`      | `diracx.routers.jobs.access_policies:WMSAccessPolicy`         |
| `gubbins-routers` | `LollygagAccessPolicy` | `gubbins.routers.lollygag.access_policy:LollygagAccessPolicy` |

## CLI Command Registration

**Entry Point Group**: `diracx.cli`

Register Typer applications as subcommands of the `dirac` CLI. Extensions can add new subcommands or extend existing ones.

### Entry Point Keys

- **`<command-name>`**: Path to Typer app (e.g., 'myext.cli.jobs:app')

### Usage Example

```toml
[project.entry-points."diracx.cli"]
jobs = "myextension.cli.jobs:app"  # Override core 'dirac jobs' command
mycmd = "myextension.cli.custom:app"  # Add 'dirac mycmd' command
```

### Important Notes

- Commands are automatically integrated into the main `dirac` CLI
- Extensions can completely replace core commands by using the same name
- Use `@app.async_command()` for async operations
- Follows standard Typer patterns for argument/option parsing

### Current Implementations

| Package       | Entry Name | Entry Point                |
| ------------- | ---------- | -------------------------- |
| `diracx-cli`  | `config`   | `diracx.cli.config:app`    |
| `diracx-cli`  | `jobs`     | `diracx.cli.jobs:app`      |
| `gubbins-cli` | `config`   | `gubbins.cli.config:app`   |
| `gubbins-cli` | `lollygag` | `gubbins.cli.lollygag:app` |

## Hidden CLI Commands

**Entry Point Group**: `diracx.cli.hidden`

Register CLI commands that should not appear in help text. Used for internal/debugging commands.

### Entry Point Keys

- **`<command-name>`**: Path to Typer app for hidden command

### Usage Example

```toml
[project.entry-points."diracx.cli.hidden"]
internal = "myextension.cli.internal:app"
debug = "myextension.cli.debug:app"
```

### Important Notes

- Commands are functional but don't appear in `dirac --help`
- Useful for debugging tools and internal utilities

### Current Implementations

| Package      | Entry Name | Entry Point               |
| ------------ | ---------- | ------------------------- |
| `diracx-cli` | `internal` | `diracx.cli.internal:app` |

## OpenSearch Database Registration

**Entry Point Group**: `diracx.dbs.os`

Register OpenSearch/Elasticsearch database classes for log and parameter storage. Connection parameters configured via `DIRACX_OS_DB_<DB_NAME>_*` environment variables.

### Entry Point Keys

- **`<db-name>`**: Path to BaseOSDB subclass (e.g., 'myext.db.os.jobs:JobParametersDB')

### Usage Example

```toml
[project.entry-points."diracx.dbs.os"]
JobParametersDB = "myextension.db.os.jobs:ExtendedJobParametersDB"
```

### Important Notes

- Database classes must inherit from `BaseOSDB`
- No automatic transaction management (unlike SQL databases)
- Connection pooling is handled by AsyncOpenSearch client

### Current Implementations

| Package     | Entry Name        | Entry Point                    |
| ----------- | ----------------- | ------------------------------ |
| `diracx-db` | `JobParametersDB` | `diracx.db.os:JobParametersDB` |

## SQL Database Registration

**Entry Point Group**: `diracx.dbs.sql`

Register SQL database classes using SQLAlchemy. Database URLs are configured via `DIRACX_DB_URL_<DB_NAME>` environment variables.

### Entry Point Keys

- **`<db-name>`**: Path to BaseSQLDB subclass (e.g., 'myext.db.sql.jobs:JobDB')

### Usage Example

```toml
[project.entry-points."diracx.dbs.sql"]
JobDB = "myextension.db.sql.jobs:ExtendedJobDB"
MyCustomDB = "myextension.db.sql.custom:MyCustomDB"
```

### Important Notes

- Database classes must inherit from `BaseSQLDB`
- Use `@declared_attr` for tables to support extension inheritance
- Transactions are auto-managed: commit on success, rollback on errors
- Connection pooling is automatic via SQLAlchemy

### Current Implementations

| Package      | Entry Name          | Entry Point                       |
| ------------ | ------------------- | --------------------------------- |
| `diracx-db`  | `AuthDB`            | `diracx.db.sql:AuthDB`            |
| `diracx-db`  | `JobDB`             | `diracx.db.sql:JobDB`             |
| `diracx-db`  | `JobLoggingDB`      | `diracx.db.sql:JobLoggingDB`      |
| `diracx-db`  | `PilotAgentsDB`     | `diracx.db.sql:PilotAgentsDB`     |
| `diracx-db`  | `SandboxMetadataDB` | `diracx.db.sql:SandboxMetadataDB` |
| `diracx-db`  | `TaskQueueDB`       | `diracx.db.sql:TaskQueueDB`       |
| `gubbins-db` | `JobDB`             | `gubbins.db.sql:GubbinsJobDB`     |
| `gubbins-db` | `LollygagDB`        | `gubbins.db.sql:LollygagDB`       |

## Minimum Client Version Declaration

**Entry Point Group**: `diracx.min_client_version`

Declare the minimum compatible client version for the server. Used to prevent compatibility issues between client and server.

### Entry Point Keys

- **`diracx`**: Variable name containing version string (e.g., 'myext.routers:MIN_VERSION')

### Usage Example

```toml
[project.entry-points."diracx.min_client_version"]
myextension = "myextension.routers:MYEXT_MIN_CLIENT_VERSION"
```

### Important Notes

- Extensions take priority over 'diracx' entry point
- Version string should follow semantic versioning
- Server rejects requests from clients below minimum version

### Current Implementations

| Package          | Entry Name | Entry Point                                |
| ---------------- | ---------- | ------------------------------------------ |
| `diracx-routers` | `diracx`   | `diracx.routers:DIRACX_MIN_CLIENT_VERSION` |

## Resource Management Functions

**Entry Point Group**: `diracx.resources`

Register functions that can be overridden by extensions to customize resource management behavior (e.g., platform compatibility).

### Entry Point Keys

- **`find_compatible_platforms`**: Function to determine platform compatibility

### Usage Example

```toml
[project.entry-points."diracx.resources"]
find_compatible_platforms = "myext.core.resources:find_compatible_platforms"
```

### Important Notes

- Uses `@supports_extending` decorator pattern
- Extension implementations automatically override core functions
- Useful for site-specific resource matching logic

### Current Implementations

| Package       | Entry Name                  | Entry Point                                       |
| ------------- | --------------------------- | ------------------------------------------------- |
| `diracx-core` | `find_compatible_platforms` | `diracx.core.resources:find_compatible_platforms` |

## FastAPI Router Registration

**Entry Point Group**: `diracx.services`

Register FastAPI routers to create new API endpoints or override existing ones. Each entry creates a route under `/api/<system-name>/`.

### Entry Point Keys

- **`<system-name>`**: Path to DiracxRouter instance (e.g., 'myext.routers.jobs:router')

### Usage Example

```toml
[project.entry-points."diracx.services"]
myjobs = "myextension.routers.jobs:router"
".well-known" = "myextension.routers.well_known:router"  # Special case: served at root
```

### Important Notes

- Routers can be disabled with `DIRACX_SERVICE_<SYSTEM_NAME>_ENABLED=false`
- Extensions can override core routers by using the same name
- All routes must have proper access policies or use `@open_access`
- The system name becomes the first tag in OpenAPI spec

### Current Implementations

| Package           | Entry Name    | Entry Point                             |
| ----------------- | ------------- | --------------------------------------- |
| `diracx-routers`  | `.well-known` | `diracx.routers.auth.well_known:router` |
| `diracx-routers`  | `auth`        | `diracx.routers.auth:router`            |
| `diracx-routers`  | `config`      | `diracx.routers.configuration:router`   |
| `diracx-routers`  | `health`      | `diracx.routers.health:router`          |
| `diracx-routers`  | `jobs`        | `diracx.routers.jobs:router`            |
| `gubbins-routers` | `.well-known` | `gubbins.routers.well_known:router`     |
| `gubbins-routers` | `lollygag`    | `gubbins.routers.lollygag:router`       |

______________________________________________________________________

*This documentation is auto-generated. See `scripts/generate_entrypoints_docs.py` for details.*
