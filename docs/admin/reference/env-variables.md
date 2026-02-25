# List of environment variables

*This page is auto-generated from the settings classes in `diracx.core.settings`.*

## AuthSettings

Settings for the authentication service.

### `DIRACX_SERVICE_AUTH_DIRAC_CLIENT_ID`

*Optional*, default value: `myDIRACClientID`

OAuth2 client identifier for DIRAC services.

This should match the client ID registered with the identity provider.

### `DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS`

*Optional*, default value: `[]`

List of allowed redirect URLs for OAuth2 authorization flow.

These URLs must be pre-registered and should match the redirect URIs
configured in the OAuth2 client registration.
Example: ["http://localhost:8000/docs/oauth2-redirect"]

### `DIRACX_SERVICE_AUTH_DEVICE_FLOW_EXPIRATION_SECONDS`

*Optional*, default value: `600`

Expiration time in seconds for device flow authorization requests.

After this time, the device code becomes invalid and users must restart
the device flow process. Default: 10 minutes.

### `DIRACX_SERVICE_AUTH_AUTHORIZATION_FLOW_EXPIRATION_SECONDS`

*Optional*, default value: `300`

Expiration time in seconds for authorization code flow.

The time window during which the authorization code remains valid
before it must be exchanged for tokens. Default: 5 minutes.

### `DIRACX_SERVICE_AUTH_STATE_KEY`

**Required**

Encryption key used to encrypt/decrypt the state parameter passed to the IAM.

This key ensures the integrity and confidentiality of state information
during OAuth2 flows. Must be a valid Fernet key.

### `DIRACX_SERVICE_AUTH_TOKEN_ISSUER`

**Required**

The issuer identifier for JWT tokens.

This should be a URI that uniquely identifies the token issuer and
matches the 'iss' claim in issued JWT tokens.

### `DIRACX_SERVICE_AUTH_TOKEN_KEYSTORE`

**Required**

Keystore containing the cryptographic keys used for signing JWT tokens.

This includes both public and private keys for token signature
generation and verification.

### `DIRACX_SERVICE_AUTH_TOKEN_ALLOWED_ALGORITHMS`

*Optional*, default value: `['RS256', 'EdDSA', 'Ed25519']`

List of allowed cryptographic algorithms for JWT token signing.

Supported algorithms include RS256 (RSA with SHA-256) and Ed25519
(Edwards-curve Digital Signature Algorithm). Default: ["RS256", "Ed25519"]

### `DIRACX_SERVICE_AUTH_ACCESS_TOKEN_EXPIRE_MINUTES`

*Optional*, default value: `20`

Expiration time in minutes for access tokens.

After this duration, access tokens become invalid and must be refreshed
or re-obtained. Default: 20 minutes.

### `DIRACX_SERVICE_AUTH_REFRESH_TOKEN_EXPIRE_MINUTES`

*Optional*, default value: `60`

Expiration time in minutes for refresh tokens.

The maximum lifetime of refresh tokens before they must be re-issued
through a new authentication flow. Default: 60 minutes.

### `DIRACX_SERVICE_AUTH_AVAILABLE_PROPERTIES`

*Optional*

Set of security properties available in this DIRAC installation.

These properties define various authorization capabilities and are used
for access control decisions. Defaults to all available security properties.

## SandboxStoreSettings

Settings for the sandbox store.

### `DIRACX_SANDBOX_STORE_BUCKET_NAME`

**Required**

Name of the S3 bucket used for storing job sandboxes.

This bucket will contain input and output sandbox files for DIRAC jobs.
The bucket must exist or auto_create_bucket must be enabled.

### `DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS`

**Required**

Configuration parameters passed to the S3 client.

### `DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET`

*Optional*, default value: `False`

Whether to automatically create the S3 bucket if it doesn't exist.

### `DIRACX_SANDBOX_STORE_URL_VALIDITY_SECONDS`

*Optional*, default value: `300`

Validity duration in seconds for pre-signed S3 URLs.

This determines how long generated download/upload URLs remain valid
before expiring. Default: 300 seconds (5 minutes).

### `DIRACX_SANDBOX_STORE_SE_NAME`

*Optional*, default value: `SandboxSE`

Logical name of the Storage Element for the sandbox store.

This name is used within DIRAC to refer to this sandbox storage
endpoint in job descriptions and file catalogs.

## OTELSettings

Settings for the Open Telemetry Configuration.

### `DIRACX_OTEL_ENABLED`

*Optional*, default value: `False`

### `DIRACX_OTEL_APPLICATION_NAME`

*Optional*, default value: `diracx`

### `DIRACX_OTEL_GRPC_ENDPOINT`

*Optional*, default value: \`\`

### `DIRACX_OTEL_GRPC_INSECURE`

*Optional*, default value: `True`

### `DIRACX_OTEL_HEADERS`

*Optional*, default value: `None`
