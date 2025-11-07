# API Reference

This section provides detailed API reference documentation for DiracX modules, automatically generated from the source code using [Griffe](https://mkdocstrings.github.io/griffe/) and [Griffe-Pydantic](https://mkdocstrings.github.io/griffe-pydantic/).

## [Core](core/index.md)

Core components including models, settings, configuration, and utilities.

- **[Models](core/models.md)** - Core Pydantic models for data validation
- **[Settings](core/settings.md)** - Configuration settings
- **[Preferences](core/preferences.md)** - User preferences
- **[Config Schema](core/config-schema.md)** - Configuration schema definitions
- **[Properties](core/properties.md)** - Security properties
- **[Extensions](core/extensions.md)** - Extension system
- **[Utilities](core/utils.md)** - Core utilities

## [Routers](routers/index.md)

FastAPI routers providing the REST API endpoints.

- **[Jobs](routers/jobs.md)** - Job management endpoints
- **[Auth](routers/auth.md)** - Authentication and authorization
- **[Configuration](routers/configuration.md)** - Configuration management
- **[Health](routers/health.md)** - Health check and monitoring

## [Logic](logic/index.md)

Business logic layer providing service implementations and orchestration.

## [Database](db/index.md)

Database models, schemas, and access layers.

- **[Job DB](db/job.md)** - Job database
- **[Job Logging DB](db/job-logging.md)** - Job logging and history
- **[Auth DB](db/auth.md)** - Authentication and authorization
- **[Sandbox Metadata DB](db/sandbox-metadata.md)** - Sandbox file metadata
- **[Task Queue DB](db/task-queue.md)** - Task queue management
- **[Pilot Agents DB](db/pilot-agents.md)** - Pilot agent tracking
- **[OpenSearch](db/opensearch.md)** - OpenSearch-based databases
- **[SQL Utilities](db/sql-utils.md)** - SQL database utilities
- **[Exceptions](db/exceptions.md)** - Database exceptions

## [CLI](cli/index.md)

Command-line interface modules and commands.

______________________________________________________________________

## How to Use

Each module page contains automatically generated documentation including:

- **Pydantic Models**: Field descriptions, types, defaults, constraints, and validation rules
- **Functions & Methods**: Parameters, return types, and docstrings
- **Type Annotations**: Full type information for all public APIs
- **Source Links**: Direct links to source code on GitHub

## Contributing Documentation

See [Writing API Docs](writing-api-docs.md) for guidelines on documenting your code.
