# Configuration

Configuration refers to the central store of configuration data which is made available to both servers and clients.
This is in contrast to "Settings" which are only available on the server side.
Confidential information (such as passwords) is only handled in Settings, see the DiracX helm chart for details.

The DiracX configuration is stored as a single YAML file.
We recommend that this is stored within a Git repository, and DiracX provides two git-based backends can be used by servers:

- `git+file`: Refers to a local git repository. This must be stored on a shared volume which is made available to all DiracX servers.
- `git+https`: Refers to a remote git repository that can be stored on any standard git host.

## Structure of the CS

The canonical way of accessing the DiracX configuration from within code is via the corresponding pydantic model.
This provides strong typing of all values in the CS and enables the schema of the `default.yml` file to be validated.
The pydantic model is defined in `diracx.core.config`.

## Client access

The DiracX configuration is made available to clients via the `/api/config/` route.
To allow for updates to be quickly and efficiently propagated to clients, DiracX respects the [`If-None-Match`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match) and [`If-Modified-Since`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Modified-Since) headers.
These headers can be used to efficiently check for updates without needing to download the entire contents of the configuration.

## Modifying configuration

Currently the canonical source of configuration is from the legacy DIRAC Configuration Service.
We foresee this will continue to be the case until the migration from DIRAC -> DiracX is complete.
During this time, the DiracX configuration is not intended to be edited directly.
The DiracX `default.yml` file differs in structure and contents from the legacy DIRAC Configuration Service.

See [how-to](../how-to/install/convert-cs.md) for how to convert.
