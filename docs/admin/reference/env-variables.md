# List of environment variables

## Core

- `DIRACX_CONFIG_BACKEND_URL`: The URL of the configuration backend.

## Services:

- `DIRACX_SERVICE_AUTH_TOKEN_ISSUER`: The issuer for the auth tokens.
- `DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS`: A JSON-encoded list of allowed redirect URIs for the authorization code
    flow.
- `DIRACX_SERVICE_AUTH_DEVICE_FLOW_EXPIRATION_SECONDS`: The expiration time for the device flow in seconds.
- `DIRACX_SERVICE_AUTH_AUTHORIZATION_FLOW_EXPIRATION_SECONDS`: The expiration time for the authorization flow in
    seconds.
- `DIRACX_SERVICE_AUTH_STATE_KEY`: The key used to encrypt the state in the authorization code flow.
- `DIRACX_SERVICE_AUTH_TOKEN_KEYSTORE`: The path to the JWKS file containing the token signing keys.
- `DIRACX_SERVICE_AUTH_TOKEN_ALLOWED_ALGORITHMS`: A JSON-encoded list of allowed algorithms for token signing.
- `DIRACX_SERVICE_AUTH_ACCESS_TOKEN_EXPIRE_MINUTES`: The expiration time for the access token in minutes.
- `DIRACX_SERVICE_AUTH_REFRESH_TOKEN_EXPIRE_MINUTES`: The expiration time for the refresh token in minutes.
- `DIRACX_SANDBOX_STORE_BUCKET_NAME`: The name of the S3 bucket for the sandbox store.
- `DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS`: A JSON-encoded dictionary of keyword arguments for the S3 client.
- `DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET`: Whether to automatically create the S3 bucket if it doesn't exist.
- `DIRACX_SANDBOX_STORE_URL_VALIDITY_SECONDS`: The validity of the presigned URLs for the sandbox store in seconds.
- `DIRACX_SANDBOX_STORE_SE_NAME`: The name of the storage element for the sandbox store.
- `DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY`: The hashed API key for the legacy exchange endpoint.
- `DIRACX_SERVICE_JOBS_ENABLED`: Whether the jobs service is enabled.

## Databases:

- `DIRACX_DB_URL_<db_name>`: The URL for the SQL database `<db_name>`.
- `DIRACX_OS_DB_<db_name>`: A JSON-encoded dictionary of connection keyword arguments for the OpenSearch database `<db_name>`

## OTEL:

- `DIRACX_OTEL_ENABLED`: Whether OpenTelemetry is enabled.
- `DIRACX_OTEL_APPLICATION_NAME`: The name of the application for OpenTelemetry.
- `DIRACX_OTEL_GRPC_ENDPOINT`: The gRPC endpoint for the OpenTelemetry collector.
- `DIRACX_OTEL_GRPC_INSECURE`: Whether to use an insecure gRPC connection for the OpenTelemetry collector.
- `DIRACX_OTEL_HEADERS`: A JSON-encoded dictionary of headers to pass to the OpenTelemetry collector.
