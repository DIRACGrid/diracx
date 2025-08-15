# Dependency injection

DiracX uses [FastAPI's dependency injection system](https://fastapi.tiangolo.com/tutorial/dependencies/) to provide dependencies to API route handlers. Dependencies are injected as function parameters using Python's `Annotated` type hints.

## Available dependencies

DiracX provides several types of dependencies that can be injected into route handlers:

### Database connections

Database connections are automatically managed through dependency injection with automatic transaction handling:

```python
from diracx.routers.dependencies import JobDB, AuthDB, JobLoggingDB


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

| Dependency          | Underlying Class                  | Description                          |
| ------------------- | --------------------------------- | ------------------------------------ |
| `JobDB`             | `diracx.db.sql.JobDB`             | Job management database              |
| `AuthDB`            | `diracx.db.sql.AuthDB`            | Authentication database              |
| `JobLoggingDB`      | `diracx.db.sql.JobLoggingDB`      | Job logging database                 |
| `PilotAgentsDB`     | `diracx.db.sql.PilotAgentsDB`     | Pilot agents database                |
| `SandboxMetadataDB` | `diracx.db.sql.SandboxMetadataDB` | Sandbox metadata database            |
| `TaskQueueDB`       | `diracx.db.sql.TaskQueueDB`       | Task queue database                  |
| `JobParametersDB`   | `diracx.db.os.JobParametersDB`    | Job parameters (OpenSearch) database |

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

Each database dependency is defined as:

```python
# SQL databases use .transaction() for automatic transaction handling
JobDB = Annotated[_JobDB, Depends(_JobDB.transaction)]

# OpenSearch databases use .session() (no automatic transactions)
JobParametersDB = Annotated[_JobParametersDB, Depends(_JobParametersDB.session)]
```

For advanced scenarios requiring explicit transaction commits (e.g., revoking tokens before returning an error):

```python
@router.post("/token")
async def token(auth_db: AuthDB, ...):
    if refresh_token_attributes["status"] == RefreshTokenStatus.REVOKED:
        # Revoke all the user tokens associated with the subject
        await auth_db.revoke_user_refresh_tokens(sub)

        # Explicitly commit to ensure revocation is saved
        await auth_db.conn.commit()

        # Raise error after commit
        raise HTTPException(status_code=401)
```

For more details on the underlying database classes, see the [Database Components](../explanations/components/db.md) documentation.

### Configuration and settings

Configuration and application settings are injected using dedicated dependencies:

```python
from diracx.routers.dependencies import Config, AuthSettings


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

| Dependency             | Underlying Class                            | Description                   |
| ---------------------- | ------------------------------------------- | ----------------------------- |
| `Config`               | `diracx.core.config.Config`                 | DiracX configuration          |
| `AuthSettings`         | `diracx.core.settings.AuthSettings`         | Authentication settings       |
| `DevelopmentSettings`  | `diracx.core.settings.DevelopmentSettings`  | Development-specific settings |
| `SandboxStoreSettings` | `diracx.core.settings.SandboxStoreSettings` | Sandbox storage settings      |

Each configuration dependency is defined as:

```python
# Configuration uses ConfigSource.create
Config = Annotated[_Config, Depends(ConfigSource.create)]

# Settings use the .create() class method
AuthSettings = Annotated[_AuthSettings, Depends(_AuthSettings.create)]
```

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

For custom settings classes that inherit from `BaseSettings`, use the `add_settings_annotation` helper from `diracx.routers.dependencies`:

```python
from diracx.routers.dependencies import add_settings_annotation
from diracx.core.settings import ServiceSettingsBase


class MyCustomSettings(ServiceSettingsBase):
    custom_option: str = "default_value"

    @classmethod
    def create(cls):
        return cls()


# Create the dependency
MySettings = add_settings_annotation(MyCustomSettings)


@router.get("/my-endpoint")
async def my_endpoint(settings: MySettings) -> dict:
    return {"custom_option": settings.custom_option}
```

### Database dependencies

Database dependencies follow the pattern of using the `.transaction()` class method:

```python
from typing import Annotated
from fastapi import Depends

# Database classes should have a .transaction() class method
MyCustomDB = Annotated[MyCustomDBClass, Depends(MyCustomDBClass.transaction)]
```

## Complete example

Here's a complete example showing multiple dependency types:

```python
from typing import Annotated
from fastapi import Depends
from diracx.routers.dependencies import Config, JobDB, AuthSettings
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
