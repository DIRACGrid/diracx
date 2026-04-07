# Dependency injection

DiracX uses [FastAPI's dependency injection system](https://fastapi.tiangolo.com/tutorial/dependencies/) to provide dependencies to API route handlers. Dependencies are injected as function parameters using Python's type hints.

DB classes, OS DB classes, and `ServiceSettingsBase` subclasses are **auto-detected** by `auto_inject_depends` in `diracx.tasks.plumbing.depends`. This means you can simply type-annotate parameters with the bare class and the framework wraps them with the appropriate `Depends` call automatically. This auto-detection is applied:

- In routers: by `DiracxRouter.add_api_route`
- In tasks: by `wrap_task` in `diracx.tasks.plumbing.factory`
- In sub-dependency functions: via the `@auto_inject` decorator

## Available dependencies

### Database connections

Database connections are automatically managed through dependency injection with automatic transaction handling. Import DB classes directly from their defining packages:

```python
from diracx.db.sql import JobDB, JobLoggingDB


@router.get("/jobs/{job_id}")
async def get_job(
    job_id: int,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
) -> JobInfo:
    # Database connections are automatically managed
    job_info = await job_db.get_job_info(job_id)
    return job_info
```

Available database dependencies:

| Class               | Package         | Description                          |
| ------------------- | --------------- | ------------------------------------ |
| `JobDB`             | `diracx.db.sql` | Job management database              |
| `AuthDB`            | `diracx.db.sql` | Authentication database              |
| `JobLoggingDB`      | `diracx.db.sql` | Job logging database                 |
| `PilotAgentsDB`     | `diracx.db.sql` | Pilot agents database                |
| `SandboxMetadataDB` | `diracx.db.sql` | Sandbox metadata database            |
| `TaskQueueDB`       | `diracx.db.sql` | Task queue database                  |
| `JobParametersDB`   | `diracx.db.os`  | Job parameters (OpenSearch) database |

#### Connection Pool Management

Database connection pools are managed through FastAPI's lifetime functions and context managers:

**SQL Databases:**

- **Engine Initialization**: At application startup, each SQL database's `engine_context()` is added to the FastAPI `lifetime_functions`
- **Connection Pool**: The `engine_context` creates an async SQLAlchemy engine with connection pooling (`pool_recycle=1800s`)
- **Pool Lifecycle**: The connection pool is created at startup and properly disposed of at shutdown
- **Per-Request Connections**: Individual connections are acquired from the pool for each request transaction

**OpenSearch Databases:**

- **Client Initialization**: Each OpenSearch database's `client_context()` is added to the FastAPI `lifetime_functions`
- **Client Pool**: The `client_context` creates an `AsyncOpenSearch` client with built-in connection pooling
- **Client Lifecycle**: The client connection pool is established at startup and closed at shutdown
- **Per-Request Sessions**: Individual sessions reuse pooled connections for each request

The `db_transaction()` function in `factory.py` manages per-request connection acquisition and includes health checking via cached database pings.

#### Transaction Management

SQL database connections have automatic transaction handling:

- **Connections** are managed through a central pool
- **Transactions** are opened for the duration of each request
- **Successful requests** (HTTP status < 400) automatically commit the transaction
- **Failed requests** (HTTP status >= 400) automatically roll back the transaction
- **Connections** are returned to the pool for reuse

The auto-detection applies the following rules:

```python
# SQL databases -> Depends(cls.transaction, scope="function")
# OpenSearch databases -> Depends(cls.session, scope="function")
# Settings classes -> Depends(cls.create)
```

For advanced scenarios requiring explicit transaction commits (e.g., revoking tokens before returning an error):

```python
from diracx.db.sql import AuthDB


@router.post("/token")
async def token(auth_db: AuthDB):
    if refresh_token_attributes["status"] == RefreshTokenStatus.REVOKED:
        # Revoke all the user tokens associated with the subject
        await auth_db.revoke_user_refresh_tokens(sub)

        # Explicitly commit to ensure revocation is saved
        await auth_db.conn.commit()

        # Raise error after commit
        raise HTTPException(status_code=401)
```

For cases where a database connection is needed without a transaction (e.g., a task that manages its own transactions in batches), use the `NoTransaction` marker:

```python
from typing import Annotated

from diracx.db.sql import SandboxMetadataDB
from diracx.tasks.plumbing.depends import NoTransaction


async def execute(
    self,
    sandbox_metadata_db: Annotated[SandboxMetadataDB, NoTransaction()],
) -> int:
    # Caller manages transactions manually
    ...
```

For more details on the underlying database classes, see the [Database Components](../explanations/components/db.md) documentation.

### Configuration and settings

Configuration and application settings are injected using dedicated dependencies. Settings classes that inherit from `ServiceSettingsBase` are auto-detected. `Config` must be imported from `diracx.routers.dependencies`:

```python
from diracx.core.settings import AuthSettings
from diracx.routers.dependencies import Config


@router.get("/config-info")
async def get_config_info(
    config: Config,
    auth_settings: AuthSettings,
) -> dict:
    return {
        "vo": config.vo,
        "token_issuer": auth_settings.token_issuer,
    }
```

Available configuration dependencies:

| Class                  | Package                       | Description                   |
| ---------------------- | ----------------------------- | ----------------------------- |
| `Config`               | `diracx.routers.dependencies` | DiracX configuration          |
| `AuthSettings`         | `diracx.core.settings`        | Authentication settings       |
| `DevelopmentSettings`  | `diracx.core.settings`        | Development-specific settings |
| `SandboxStoreSettings` | `diracx.core.settings`        | Sandbox storage settings      |

`Config` is special because it doesn't inherit from `ServiceSettingsBase` and uses `ConfigSource.create` as its dependency factory. It is the only dependency that requires importing a pre-wrapped `Annotated` type.

For more details on configuration and settings classes, see the [Configuration](configuration.md) documentation.

### User authentication and authorization

User information and authentication are handled through specialized dependencies:

```python
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token
from diracx.routers.auth.utils import has_properties
from diracx.core.properties import JOB_ADMINISTRATOR


@router.post("/admin-action")
async def admin_action(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    _: Annotated[None, has_properties(JOB_ADMINISTRATOR)],
) -> dict:
    return {"user": user_info.preferred_username, "vo": user_info.vo}
```

Authentication dependencies:

| Function                    | Module                       | Description                                      |
| --------------------------- | ---------------------------- | ------------------------------------------------ |
| `verify_dirac_access_token` | `diracx.routers.utils.users` | Verifies JWT tokens and returns user information |
| `has_properties(property)`  | `diracx.routers.auth.utils`  | Checks if user has specific DIRAC properties     |

These functions handle JWT token validation and property-based authorization checks.

### Security properties

Security properties can be injected to determine what properties are available:

```python
from diracx.routers.dependencies import AvailableSecurityProperties


@router.get("/available-properties")
async def get_properties(
    properties: AvailableSecurityProperties,
) -> list[str]:
    return [prop.value for prop in properties]
```

### Access policies

Access policies provide fine-grained authorization control:

```python
from diracx.db.sql import JobDB
from diracx.routers.jobs.access_policies import CheckWMSPolicyCallable, ActionType


@router.post("/jobs")
async def create_job(
    job_definition: str,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict:
    # Check if user can create jobs
    await check_permissions(action=ActionType.CREATE, job_db=job_db)
    # Proceed with job creation
    ...
```

## Creating custom dependencies

### Settings dependencies

Custom settings classes that inherit from `ServiceSettingsBase` are auto-detected. Simply define the class and use it as a type annotation:

```python
from diracx.core.settings import ServiceSettingsBase


class MyCustomSettings(ServiceSettingsBase):
    custom_option: str = "default_value"

    @classmethod
    def create(cls):
        return cls()


@router.get("/my-endpoint")
async def my_endpoint(settings: MyCustomSettings) -> dict:
    return {"custom_option": settings.custom_option}
```

### Database dependencies

Database dependencies are also auto-detected. Any `BaseSQLDB` subclass used as a type annotation will automatically get wrapped with `Depends(cls.transaction, scope="function")`:

```python
from my_extension.db.sql import MyCustomDB


@router.get("/my-data")
async def get_data(db: MyCustomDB) -> dict:
    ...
```

## Complete example

Here's a complete example showing multiple dependency types:

```python
from diracx.core.settings import AuthSettings
from diracx.db.sql import JobDB
from diracx.routers.dependencies import Config
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token
from diracx.routers.auth.utils import has_properties
from diracx.core.properties import NORMAL_USER


@router.post("/submit-job")
async def submit_job(
    job_definition: str,
    config: Config,
    auth_settings: AuthSettings,
    job_db: JobDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    _: Annotated[None, has_properties(NORMAL_USER)],
) -> dict:
    """Submit a job with full dependency injection."""
    # All dependencies are automatically injected and managed
    job_id = await job_db.insert_job(job_definition, user_info.preferred_username)

    return {
        "job_id": job_id,
        "submitted_by": user_info.preferred_username,
        "vo": config.vo,
    }
```

## Dependency lifecycle

- **SQL Database connections**: Connection pooling with automatic transaction handling per request
    - Transactions opened at request start
    - Auto-commit on success (HTTP status < 400)
    - Auto-rollback on failure (HTTP status >= 400)
- **OpenSearch Database connections**: Connection pooling without automatic transactions
- **Settings**: Instantiated once and reused across requests
- **User authentication**: JWT token validated on each request
- **Configuration**: Loaded once at startup and cached, with automatic refresh
