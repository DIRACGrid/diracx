### Configuring a client

TODO: check if it can go in a file

Required environment variables to interact with the services:

- `DIRACX_URL`: the URL pointing to diracx services
- `DIRACX_CA_PATH`: CA path used by the diracx services

Optional environment variables:

- `DIRACX_OUTPUT_FORMAT`: output format (e.g. `JSON`). Default value depends whether the output stream is associated to a terminal.
- `DIRACX_LOG_LEVEL`: logging level (e.g. `ERROR`). Defaults to `INFO`.
- `DIRACX_CREDENTIALS_PATH`: path where access and refresh tokens are stored. Defaults to `~/.cache/diracx/credentials.json`.
