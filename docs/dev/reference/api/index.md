# API Reference

This section provides detailed API reference documentation for DiracX modules, automatically generated from the source code using [Griffe](https://mkdocstrings.github.io/griffe/) and [Griffe-Pydantic](https://mkdocstrings.github.io/griffe-pydantic/).

## [Core](core/index.md)

Core components including models, settings, configuration, and utilities.

        - **[Models](core/models.md)** - Core Pydantic models for data validation
        - **[Settings](core/settings.md)** - Configuration settings
        - **[Preferences](core/preferences.md)** - User preferences
        - **[Configuration](core/config.md)** - Configuration schema and sources
        - **[Exceptions](core/exceptions.md)** - Core exception classes
        - **[Resources](core/resources.md)** - Resource management and dependency injection
        - **[S3](core/s3.md)** - S3-compatible object storage integration
        - **[Properties](core/properties.md)** - Security properties
        - **[Extensions](core/extensions.md)** - Extension system
        - **[Utilities](core/utils.md)** - Core utilities

## [Routers](routers/index.md)

FastAPI routers providing the REST API endpoints.

        - **[Jobs](routers/jobs.md)** - Job management endpoints
        - **[Auth](routers/auth.md)** - Authentication and authorization
        - **[Configuration](routers/configuration.md)** - Configuration management
        - **[Health](routers/health.md)** - Health check and monitoring
        - **[Access Policies](routers/access_policies.md)** - Access control policies
        - **[Dependencies](routers/dependencies.md)** - FastAPI dependency injection utilities
        - **[Factory](routers/factory.md)** - Router factory functions
        - **[FastAPI Classes](routers/fastapi_classes.md)** - Custom FastAPI router classes
        - **[OpenTelemetry](routers/otel.md)** - Tracing and instrumentation
        - **[Utilities](routers/utils.md)** - Router utilities

## [Logic](logic/index.md)

Business logic layer providing service implementations and orchestration.

## [Database](db/index.md)

Database models, schemas, and access layers.

        - **[Job DB](db/job.md)** - Job database
        - **[Job Logging DB](db/job_logging.md)** - Job logging and history
        - **[Auth DB](db/auth.md)** - Authentication and authorization
        - **[Sandbox Metadata DB](db/sandbox_metadata.md)** - Sandbox file metadata
        - **[Task Queue DB](db/task_queue.md)** - Task queue management
        - **[Pilot Agents DB](db/pilot_agents.md)** - Pilot agent tracking
        - **[Dummy DB](db/dummy.md)** - Dummy database for testing
        - **[OpenSearch](db/opensearch.md)** - OpenSearch-based databases
        - **[SQL Utilities](db/utils.md)** - SQL database utilities
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
