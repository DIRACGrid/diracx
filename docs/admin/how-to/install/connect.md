# Connect DIRAC to DiracX

DIRAC needs to be able to "talk" to DiracX. The explanation of why this is needed can be found [here](../../explanations/auth-with-diracx.md#dirac-diracx-communication)

Proceed with generating a legacy exchange api key:

```python
import secrets
import base64
import hashlib

token = secrets.token_bytes()
# This is the secret to include in the request by setting the
# /DiracX/LegacyExchangeApiKey CS option in your legacy DIRAC installation (in the local -- secluded -- dirac.cfg file)
print(f"API key is diracx:legacy:{base64.urlsafe_b64encode(token).decode()}")

# This is the environment variable to set on the DiracX server
print(f"DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY={hashlib.sha256(token).hexdigest()}")
```

now:

- set the `Diracx/LegacyExchangeApiKey` option in the *dirac.cfg* that is **local** to the DIRAC machines (normally under `/opt/dirac/etc/dirac.cfg`).
- set the env variable `DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY`
    (if you running DiracX using K8, set the same value to the key under `diracx.settings` in the charts).
