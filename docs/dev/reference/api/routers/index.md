# Routers

The routers module provides the infrastructure for building FastAPI REST API endpoints in DiracX. This section documents the tools and utilities developers need to add new routes, implement access control, and structure their API endpoints.

## For REST API Documentation

If you're looking for documentation on the **available REST API endpoints** (HTTP methods, request/response schemas, etc.), please see:

- **[REST API Routes](../../routes/index.md)** - Complete REST API documentation with request/response examples

When running a DiracX instance, you can also access:

- **Swagger UI** at `/api/docs` - Interactive API documentation where you can try endpoints
- **OpenAPI Specification** at `/api/openapi.json` - Machine-readable API schema

## Developer Infrastructure

This section documents the Python infrastructure for **building** routers:

### Core Infrastructure

- **[Dependencies](dependencies.md)** - FastAPI dependency injection utilities for database access, authentication, configuration
- **[Factory](factory.md)** - Router factory functions for creating and registering routers
- **[FastAPI Classes](fastapi_classes.md)** - Custom DiracX router classes that extend FastAPI functionality
- **[Access Policies](access_policies.md)** - Access control and authorization policy implementations
- **[Utilities](utils.md)** - Helper functions and utilities for building routes

### OpenTelemetry Integration

- **[OpenTelemetry](otel.md)** - Instrumentation and tracing setup for monitoring API requests

### Example Routers

For reference on how routers are structured, you can examine the source code of existing routers:

- `diracx.routers.jobs` - Job management endpoints
- `diracx.routers.auth` - Authentication flows
- `diracx.routers.configuration` - Configuration management
- `diracx.routers.job_manager` - Advanced job operations

**Note**: For complete REST API endpoint documentation with request/response examples, see the **[REST API Routes](../../routes/index.md)** section.
