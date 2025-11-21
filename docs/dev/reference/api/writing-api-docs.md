# Writing API Documentation

This guide explains how to write documentation that will be automatically picked up by Griffe and rendered in the API reference.

## Docstring Style

DiracX uses **Google-style docstrings**. Here's the general format:

```python
def example_function(param1: str, param2: int = 0) -> dict[str, Any]:
    """Brief one-line summary.

    Longer description of the function if needed. This can span
    multiple paragraphs.

    Args:
        param1: Description of param1.
        param2: Description of param2. Defaults to 0.

    Returns:
        Description of the return value.

    Raises:
        ValueError: When param2 is negative.
        TypeError: When param1 is not a string.

    Examples:
        >>> example_function("test", 42)
        {'result': 'test_42'}
    """
    if param2 < 0:
        raise ValueError("param2 must be non-negative")
    return {"result": f"{param1}_{param2}"}
```

## Documenting Pydantic Models

With Griffe-Pydantic, your Pydantic models are automatically documented with rich information:

```python
from pydantic import BaseModel, Field


class UserConfig(BaseModel):
    """User configuration model.

    This model represents a user's configuration in the DiracX system.
    """

    username: str = Field(
        ...,
        description="The user's unique username",
        min_length=3,
        max_length=50,
        pattern="^[a-zA-Z0-9_-]+$",
    )

    email: str | None = Field(
        None, description="User's email address for notifications"
    )

    age: int = Field(default=0, description="User's age", ge=0, le=150)

    roles: list[str] = Field(
        default_factory=list, description="List of roles assigned to the user"
    )
```

### What Gets Documented Automatically

Griffe-Pydantic extracts and displays:

- **Field names and types** (including Union types, Optional, etc.)
- **Field descriptions** from `Field(description=...)`
- **Default values** from `Field(default=...)`
- **Validation constraints**: `min_length`, `max_length`, `ge`, `le`, `pattern`, etc.
- **Required vs optional** fields
- **Field examples** from `Field(examples=...)`
- **Validators** (custom field validators)

## Class Documentation

```python
class MyService:
    """Service for handling business logic.

    This service provides methods for processing data and
    interacting with the database.

    Attributes:
        db: The database connection.
        cache: Optional cache instance.
    """

    def __init__(self, db: Database, cache: Cache | None = None):
        """Initialize the service.

        Args:
            db: Database connection instance.
            cache: Optional cache for performance optimization.
        """
        self.db = db
        self.cache = cache

    async def process_data(self, data: dict[str, Any]) -> ProcessResult:
        """Process the input data.

        Args:
            data: Raw data dictionary to process.

        Returns:
            Processed result with metadata.

        Raises:
            ValidationError: If data is invalid.
            DatabaseError: If database operation fails.
        """
        # Implementation here
        pass
```

## Module Documentation

Add module-level documentation at the top of your Python files:

```python
"""Job management utilities.

This module provides utilities for managing jobs in DiracX,
including submission, monitoring, and status updates.

Example:
    >>> from diracx.routers.jobs import submit_job
    >>> result = await submit_job(job_data)
"""

from __future__ import annotations

# Rest of your code...
```

## Auto-Discovery of Classes and Functions

The `::: module.path` syntax automatically discovers and documents all public members.

### What Gets Auto-Discovered

By default, mkdocstrings will discover and document:

1. **Functions**: All functions not starting with `_`
2. **Classes**: All classes not starting with `_`
3. **Constants**: Module-level constants
4. **Pydantic Models**: With field information from griffe-pydantic

### Controlling What's Documented

#### Show Everything (Including Undocumented)

```markdown
::: diracx.core.models
    options:
      show_if_no_docstring: true
      filters:
        - "!^_"  # Exclude private members
```

#### Show Only Specific Members

```markdown
::: diracx.core.models
    options:
      members:
        - JobStatus
        - UserInfo
        - SandboxInfo
```

#### Exclude Specific Members

```markdown
::: diracx.routers.jobs
    options:
      filters:
        - "!^_"        # Exclude private members
        - "!^logger"   # Exclude logger
        - "!router"    # Exclude router instance
```

#### Show Inherited Members

```markdown
::: diracx.core.settings.ServiceSettingsBase
    options:
      inherited_members: true
      show_bases: true
```

### Common Patterns

#### Document a Whole Module

```markdown
# My Module

::: diracx.module.name
    options:
      show_root_heading: true
      show_source: true
      members_order: source
      group_by_category: true
```

This will show:

- Module docstring
- All public classes
- All public functions
- All public constants
- Grouped by category (attributes, classes, functions, etc.)

#### Document a Specific Class

```markdown
# JobStatus Model

::: diracx.core.models.JobStatus
    options:
      show_root_heading: true
      members_order: alphabetical
```

#### Document Multiple Related Classes

```markdown
# Job Models

## JobStatus

::: diracx.core.models.JobStatus

## JobInfo

::: diracx.core.models.JobInfo

## JobSubmission

::: diracx.core.models.JobSubmission
```

### Debugging Empty Documentation

If a module page appears empty, check:

1. **Module exists and is importable**

    ```bash
    pixi run -e mkdocs python -c "import diracx.module.name; print(dir(diracx.module.name))"
    ```

2. **Members are public (not starting with `_`)**

    ```python
    # This will be documented
    def public_function():
        pass


    # This will NOT be documented
    def _private_function():
        pass
    ```

3. **Set `show_if_no_docstring: true`** to see undocumented members

4. **Check the build output** for import errors or warnings

### Example: Full Router Documentation

Here's how to document a complete router with all its routes:

```markdown
# Jobs Router

Job management endpoints.

## Router Module

::: diracx.routers.jobs
    options:
      show_root_heading: true
      show_source: true
      show_if_no_docstring: true
      filters:
        - "!^_"
        - "!^logger"

## Submission Routes

::: diracx.routers.jobs.submission
    options:
      show_root_heading: false
      members_order: source
```

This will show:

- The router's docstring
- All route handler functions with their HTTP methods, paths, and parameters
- Request/response models
- Dependencies
- Source code links

## Best Practices

### 1. Be Descriptive but Concise

```python
# Good
def calculate_total(amounts: list[float]) -> float:
    """Calculate the sum of all amounts.

    Args:
        amounts: List of numeric amounts to sum.

    Returns:
        The total sum of all amounts.
    """
    return sum(amounts)


# Avoid - too brief
def calculate_total(amounts: list[float]) -> float:
    """Calculate total."""
    return sum(amounts)
```

### 2. Document Type Information

Even though type hints are extracted automatically, explain complex types:

```python
def process_config(config: dict[str, dict[str, list[str]]]) -> ProcessedConfig:
    """Process the configuration dictionary.

    Args:
        config: Configuration mapping where keys are section names,
            values are dictionaries mapping setting names to lists
            of allowed values.

    Returns:
        Validated and processed configuration object.
    """
    pass
```

### 3. Use Examples

Examples help users understand how to use your code:

```python
def recursive_merge(base: Any, override: Any) -> Any:
    """Recursively merge dictionaries; values in ``override`` take precedence.

    - If both ``base`` and ``override`` are dicts, merge keys recursively.
    - Otherwise, return ``override`` if it is not ``None``; fallback to ``base``.

    Args:
        base: Base dictionary or value to merge.
        override: Override dictionary or value to merge. Values here take precedence.

    Returns:
        The merged result.

    Examples:
        >>> from diracx.core.utils import recursive_merge
        >>> base = {"a": 1, "b": {"c": 2, "d": 3}}
        >>> override = {"b": {"c": 10}, "e": 4}
        >>> recursive_merge(base, override)
        {'a': 1, 'b': {'c': 10, 'd': 3}, 'e': 4}
        >>> recursive_merge(None, {"key": "value"})
        {'key': 'value'}
    """
    if isinstance(base, dict) and isinstance(override, dict):
        merged: dict[str, Any] = {}
        for key, base_val in base.items():
            if key in override:
                merged[key] = recursive_merge(base_val, override[key])
            else:
                merged[key] = base_val
        for key, override_val in override.items():
            if key not in merged:
                merged[key] = override_val
        return merged
    return override if override is not None else base
```

### 4. Document Async Functions

Be clear about async behavior:

```python
async def fetch_user_data(user_id: int) -> UserData:
    """Fetch user data from the database.

    This is an async function that queries the database and returns
    user information. Await this function when calling.

    Args:
        user_id: The unique identifier for the user.

    Returns:
        User data object with all user information.

    Raises:
        NotFoundError: If user doesn't exist.
        DatabaseError: If database query fails.
    """
    pass
```

## Testing Your Documentation

To test your documentation locally:

```bash
# Install the mkdocs environment
pixi install -e mkdocs

# Serve the documentation locally
pixi run -e mkdocs mkdocs serve

# Build the documentation (with strict mode)
pixi run -e mkdocs mkdocs-build
```

Then visit `http://127.0.0.1:8000` to see your documentation.

## Excluding Private Members

By default, members starting with `_` are excluded from documentation. To explicitly control what's documented:

```python
class MyClass:
    """Public class."""

    def public_method(self):
        """This will be documented."""
        pass

    def _private_method(self):
        """This won't be documented (starts with _)."""
        pass

    def __dunder_method__(self):
        """This won't be documented (dunder method)."""
        pass
```

## Additional Resources

- [Google Style Docstrings Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- [Griffe Documentation](https://mkdocstrings.github.io/griffe/)
- [Griffe-Pydantic Documentation](https://mkdocstrings.github.io/griffe-pydantic/)
- [mkdocstrings Documentation](https://mkdocstrings.github.io/)
