# RSS Read Adapter

This module provides a read adapter that calls the diracx RSS API and returns data in the legacy format expected by the ResourceStatus and SiteStatus singletons' cache refresh methods.

## Features

- Client calls to diracx RSS API endpoints
- Response translation from diracx Pydantic models to legacy format
- Support for all resource types: StorageElement, ComputingElement, FTS, and Site

## Components

- `response_translation.py`: Translates responses to legacy format
- `statuses.py`: Get statuses from the RSS API and translate them
- `__init__.py`: Main module interface

## Testing

Run tests with:
```bash
pixi run pytest-rss-read-adapter
```