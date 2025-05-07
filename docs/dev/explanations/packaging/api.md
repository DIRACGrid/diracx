## diracx-api

The `diracx-api` provides a Python API for interacting with services, leveraging the `diracx-client`.

### API Methods

API methods are located in `diracx-api/src/diracx/api/`. To create an API method:

1. Import `AsyncDiracClient`.
2. Decorate the method with `@with_client` to handle client configuration.
3. Pass the `client` as a keyword argument.

#### Example

```python
from diracx.client.aio import AsyncDiracClient
from .utils import with_client


@with_client
async def create_sandbox(paths: list[Path], *, client: AsyncDiracClient) -> str: ...
```

In this example, `paths` are the parameters of the API. The `@with_client` decorator allows the method to be called without manually managing the client:

```python
# Managed by @with_client
# Useful for basic work requiring a single call to the service
result = await create_sandbox(paths)

# For optimised performance with multiple service interactions
async with AsyncDiracClient() as client:
    result = await create_sandbox(paths, client)
```
