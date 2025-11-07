# API Reference Documentation

This directory contains auto-generated API reference documentation for DiracX packages.

## Structure

The API reference is organized by package:

- **core/** - Core functionality (models, settings, configuration)
- **routers/** - FastAPI routers and endpoints
- **logic/** - Business logic layer
- **db/** - Database models and access layers
- **cli/** - Command-line interface

## Maintaining Documentation

### Checking Coverage

To ensure all modules are documented, run the coverage check script:

```bash
cd docs/dev/reference/api
python check_coverage.py
```

This will report any Python modules that are missing documentation pages.

### Adding Documentation for New Modules

When you add a new Python module, you need to add corresponding documentation:

1. **Create a new .md file** in the appropriate section directory (core/, routers/, logic/, db/, or cli/)

2. **Add module references** using the `:::` syntax:

    ```markdown
    # My New Module

    Description of what this module does.

    ::: diracx.package.module
        options:
          show_root_heading: true
          show_source: true
          members_order: source
          group_by_category: true
          show_if_no_docstring: true
    ```

3. **Update the navigation** in `mkdocs.yml`:

    ```yaml
    - Logic:
      - dev/reference/api/logic/index.md
      - Jobs: dev/reference/api/logic/jobs.md
      - Auth: dev/reference/api/logic/auth.md
      - My New Module: dev/reference/api/logic/my-new-module.md  # Add here
    ```

4. **Update the index page** (e.g., `logic/index.md`) to link to your new page

5. **Run the coverage check** to verify your documentation is complete

### Documentation Options

The `:::` directive supports various options to control how documentation is rendered:

- `show_root_heading: true` - Show the module name as a heading
- `show_source: true` - Show source code links
- `members_order: source` - Order members as they appear in source
- `group_by_category: true` - Group by functions, classes, etc.
- `show_if_no_docstring: true` - Show members even without docstrings
- `filters: ["!^_"]` - Hide private members (starting with \_)

For more options, see the [mkdocstrings-python documentation](https://mkdocstrings.github.io/python/usage/).

## Writing Good Docstrings

The API reference is auto-generated from docstrings in the source code. Follow these guidelines:

1. **Use Google-style docstrings**:

    ```python
    def my_function(arg1: str, arg2: int) -> bool:
        """Brief description of the function.

        More detailed description if needed. This can span
        multiple lines.

        Args:
            arg1: Description of arg1
            arg2: Description of arg2

        Returns:
            Description of return value

        Raises:
            ValueError: When arg2 is negative
        """
    ```

2. **Document Pydantic models** using Field descriptions:

    ```python
    from pydantic import BaseModel, Field


    class MyModel(BaseModel):
        """Brief description of the model."""

        name: str = Field(..., description="The name field")
        age: int = Field(..., ge=0, description="Age must be non-negative")
    ```

3. **Document FastAPI endpoints** with clear descriptions:

    ```python
    @router.post("/jobs")
    async def submit_job(job: JobDefinition) -> InsertedJob:
        """Submit a new job to the system.

        This endpoint accepts a job definition and submits it
        to the task queue for processing.

        Args:
            job: The job definition to submit

        Returns:
            Information about the inserted job including job ID
        """
    ```

## Troubleshooting

### Module not showing up

If a module isn't rendering in the docs:

1. Check that the module path is correct (must point to actual `.py` files, not empty `__init__.py`)
2. Verify the module is in the `paths` list in `mkdocs.yml` under the mkdocstrings config
3. Run `python check_coverage.py` to see if it's detected

### Decorators not showing

FastAPI route decorators (like `@router.post("/path")`) are visible in the "Source code" section when you expand it. They are not displayed separately by default.

### Empty documentation

If a module shows up but has no content:

1. Check that the module actually has functions/classes (not just an empty `__init__.py`)
2. Ensure docstrings are present in the source code
3. Use `show_if_no_docstring: true` to show members even without docs

## Setup

The API reference is generated using:

- **mkdocstrings** - Plugin for generating API documentation
- **mkdocstrings-python** - Python handler for mkdocstrings
- **griffe** - Python code parser
- **griffe-pydantic** - Extension for enhanced Pydantic model documentation

Configuration is in `mkdocs.yml` (search for `mkdocstrings` plugin).
