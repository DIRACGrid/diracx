# List of environment variables

*This page is auto-generated from the DevelopmentSettings class in `diracx.core.settings`.*

## DevelopmentSettings

Settings for the Development Configuration that can influence run time.

### `DIRACX_DEV_CRASH_ON_MISSED_ACCESS_POLICY`

*Optional*, default value: `False`

When set to true (only for demo/CI), crash if an access policy isn't called.

This is useful for development and testing to ensure all endpoints have proper
access control policies defined.
