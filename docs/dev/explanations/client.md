## Overview

The DiracX client is a comprehensive toolset designed to interact with various services. It consists of three main components:

```
┌────────┐
│ client │
└────┬───┘
  ┌──▼──┐
  │ api │
  └──┬──┘
  ┌──▼──┐
  │ cli │
  └─────┘
```

1. **diracx-client**: A client library generated from OpenAPI specifications.
2. **diracx-api**: A Python API to interact with services using the diracx-client.
3. **diracx-cli**: A command-line interface for direct interaction with the services.

## diracx-client

The `diracx-client` consists of three parts:

- an auto-generated client library that facilitates communication with services defined by OpenAPI specifications. (the `generated` folder)
- customization, in the `patches` folder, which mirror the structure of the generated client.
- the base modules (`aio`, `extensions`, `models`) just exporting what we want to be exporting

`diracx-client` also defines a `AsyncDiracClient` class which exposes all these low level calls, and handles the authentication/authorisation aspects, as well as the interactions with extensions.

### Generating a Client

The client is generated using [AutoRest](https://github.com/Azure/autorest), a tool that reads OpenAPI configurations provided by FastAPI routers.

- **Breaking Changes**: Each time there is a breaking change in a router, the client needs to be regenerated.

### Updating the Client

The CI/CD pipeline handles client regeneration upon each push to the `main` branch. This process helps detect breaking changes in the developer's code, causing the CI/CD to fail if such changes are present.

If a breaking change is acknowledged and approved, one of the repo admin will regenerate the client on behalf of the developer. Developers can still manually regenerate the client but it requires a few additional tools. To see how to generate a client consult the [how-to](../how-to/client-generation.md).

### Structure of the Generated Client

TODO: check it is still valid, integrate extensions into it

The generated client consists of several key components:

- **models**: Represent the data structures.
- **operations**: Contain the methods to interact with the API endpoints.
- **aio**: Asynchronous client.

Further details can be found in the [Python Autorest documentation](https://github.com/Azure/autorest.python/blob/main/docs/client/readme.md).

To customize a client, see [how-to](../how-to/client-customization.md)

### Configuring the Generated Client

Clients need to be configured to interact with services. This is performed through **DiracxPreferences**, which is a [BaseSettings Pydantic model](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) that load configuration from the environment.

### Getting preferences

Developers can get access to the preferences through the following method:

```python
from diracx.core.preferences import get_diracx_preferences

...

credentials_path = get_diracx_preferences().credentials_path
```

Note: preferences are cached.
