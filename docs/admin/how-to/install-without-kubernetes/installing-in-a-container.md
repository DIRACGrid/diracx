# Installing DiracX in a container

!!! warning "This is for container deployment only. For kubernetes deployment please refer to [Installing DiracX](/docs/admin/how-to/install/installing.md)."

## Notes

- We are running a 'developer' setup where we can edit the diracx code directly and run the local code
    inside the container. This really helps with debugging ;-)
- Similar to DIRAC this is installed as the (unprivileged) dirac user (and not root). There might be limitations to the approach that we have not found yet.
- We give the version numbers of the code we tested, you should always check for the latest version and install this. If in doubt, please as on the DiracX [mattermost channel](https://mattermost.web.cern.ch/diracx/channels/town-square). In this example we used DIRAC v9.0.2, diracx 0.0.12 and diracx-web v0.1.0-a10.
- If the same version number has to be specified in two different places, we indicate this explicitly.
- We keep all of our configuration files in a folder called dirac-container. This name is chosen arbitrarily, and can be replaced with a name of your choice. However, the configuration examples assume that all files needed are kept in the same directory.

## Configuration Files

There are four configuration files that need to be provided to install DiracX in a container:
`diracx.env`, `diracx-web.env`, `podman-compose.yaml` and `jwks.json`.
In addition we also overwrite the entry point to include the database initialisation/schema management.
You also need your OpenSearch server's `/etc/opensearch/root-ca.crt` (here called opensearch-ca.pem) to be able to connect to your OpenSearch server.
Both `entrypoint.sh` and `opensearch-ca.pem` should be placed in the same folder as the configuration files.

`diracx.env` is based on [reference/env-variables](../../reference/env-variables.md) <br>
`jwks.json` is used to to generate the diracx authentication tokens. Please also see [how to rotate a secret](../../how-to/rotate-a-secret.md) <br>
`podman-compose.yaml` is the container steering/manifest file. <br>

### jwks.json

To generate the jwks.json use a diracx container and start it with a shell (code adapted from `diracx/run_local.sh`): <br>
`podman run --rm -ti ghcr.io/diracgrid/diracx/services:v0.0.12 /bin/bash`
The --rm removes the container automatically after exiting.
`python -m diracx.logic rotate-jwk --jwks-path "/tmp/jwks.json"`
Then copy content of /tmp/jwks.json to file outside of the container to be mapped in a volume to the real thing.

### diracx.env

At this point you need to have created the `DiracXAuthDB` and the `jwks.json` (see above).
Now you need two further keys:

`DIRACX_SERVICE_AUTH_STATE_KEY`: Please see also [env-variables/diracx_service_auth_state_key](../../reference/env-variables.md#diracx_service_auth_state_key) and [dynamic-secrets-diracx-dynamic-secrets](../../explanations/chart-structure.md#dynamic-secrets-diracx-dynamic-secrets)
To generate the key we follow diracx/run_local.sh: `state_key="$(head -c 32 /dev/urandom | base64)"`

`DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY`:
This is adapted from ["generating a legacy exchange api key"] (connect.md) ("generating a legacy exchange api key") goes into dirac.cfg (as diracx:legacy...) and into diracx.env as a hash, but the gist of it is, the shared secret allows dirac and diracx to communicate.
To generate the legacy key, you can use the following python snippet:

```
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

Below is an example of a dirac.env file. The server is called 'diractest.grid.hep.ph.ic.ac.uk'. Replace as necessary.

```
DIRACX_SERVICE_AUTH_STATE_KEY=[state_key from above]
DIRACX_SERVICE_AUTH_TOKEN_ISSUER=["Your hostname here", e.g. "https://diractest.grid.hep.ph.ic.ac.uk"]
DIRACX_SERVICE_AUTH_TOKEN_KEYSTORE=file:///keystore/jwks.json [The one you made earlier]
# replace diractest.grid.hep.ph.ic.ac.uk with your hostname
DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS='["https://diractest.grid.hep.ph.ic.ac.uk/#authentication-callback"]'
# we are currently not using S3, nevertheless these variables need to be set
DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS="{}"
DIRACX_SANDBOX_STORE_BUCKET_NAME="notsetyet"
# disable the jobs router (and hopefully S3)
DIRACX_SERVICE_JOBS_ENABLED=false
# we are using a local git repo on the node, this is not fully supported yet.
# Please see: https://diracx.io/en/latest/RUN_PROD/#cs for the approved way.
DIRACX_CONFIG_BACKEND_URL="git+file:///cs_store?revision=main"
DIRACX_DB_URL_AUTHDB=mysql+aiomysql://YourUser:YourPassword@host.containers.internal:3306/DiracXAuthDB
# these are the users and passwords you have chosen for the databases in DIRAC; they can be found in
# dirac.cfg
# host.containers.internal: For the development node our databases are on the same machine
DIRACX_DB_URL_JOBDB=mysql+aiomysql://YourUser:YourPassword@host.containers.internal:3306/JobDB
DIRACX_DB_URL_JOBLOGGINGDB=mysql+aiomysql://YourUser:YourPassword@host.containers.internal:3306/JobLoggingDB
DIRACX_DB_URL_PILOTAGENTSDB=mysql+aiomysql://YourUser:YourPassword@host.containers.internal:3306/PilotAgentsDB
DIRACX_DB_URL_SANDBOXMETADATADB=mysql+aiomysql://YourUser:YourPassword@host.containers.internal:3306/SandboxMetadataDB
DIRACX_DB_URL_TASKQUEUEDB=mysql+aiomysql://YourUser:YourPassword@host.containers.internal:3306/TaskQueueDB
# opensearch related; the user and password are specific to opensearch,
DIRACX_OS_DB_JOBPARAMETERSDB={"hosts":"YourOpensearchUsername:YourOpensearchPwd@YourOpensearchServer:9200", "use_ssl":true, "ca_certs":"/etc/opensearch-ca.pem"}
DIRACX_OS_DB_PILOTLOGSDB={"hosts":"YourOpensearchUsername:YourOpensearchPwd@YourOpenSearchServer:9200", "use_ssl":true, "ca_certs":"/etc/opensearch-ca.pem"}
DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY=(from the output from generating the legacy key)
```

### diracx-web.env

This file contains one line:
`DIRACX_URL=https://diractest.grid.hep.ph.ic.ac.uk/api`

### podman-compose.yaml

Note: Please check that your ports specified in the yaml file match the ports you set up during the preparing a node for a container install procedure. The DiracX version specified must match the ones you use when you install the code. The `uvicorn` command is what actually starts diracx services. <br>
Sources: <br>
`ghcr.io`: from https://diracx.diracgrid.org/en/latest/RUN_PROD/#diracx-service-configuration <br>
`command`: from diracx-charts/diracx/templates/diracx/deployment.yaml <br>
`/opt/dirac/diracx-config:/cs_store:z,rw`: only needed because we use a local repo for the CS, a remote repo would be read directly over https. <br>

```
---
services:
  diracx-services:
    ports:
      - 127.0.0.1:8000:8000
    image: ghcr.io/diracgrid/diracx/services:v0.0.12
    env_file: "diracx.env"
    command: "uvicorn --factory diracx.routers:create_app --host=0.0.0.0 --port=8000 --proxy-headers --forwarded-allow-ips=*"
    volumes:
      - ./entrypoint.sh:/entrypoint.sh:z,ro
      - ./jwks.json:/keystore/jwks.json:z,ro
      - /opt/dirac/diracx-config:/cs_store:z,rw
      - /opt/dirac/diracx:/diracx_sources:z,ro
      - ./opensearch-ca.pem:/etc/opensearch-ca.pem:z,ro
  diracx-web:
    image: ghcr.io/diracgrid/diracx-web/static:v0.1.0-a10
    ports:
      - 127.0.0.1:8001:8080
    env_file: "diracx-web.env"
```

### entrypoint.sh

```bash
#!/bin/bash

set -e
echo "Welcome to the dark side"

if [ -f /activate.sh ]; then
    source /activate.sh
else
    eval "$(micromamba shell hook --shell=posix)" && micromamba activate base
fi

# this sets it up so that we can make changes to the code
pip install -e /diracx_sources/diracx-core \
            -e /diracx_sources/diracx-db \
            -e /diracx_sources/diracx-logic \
            -e /diracx_sources/diracx-routers \
            -e /diracx_sources/diracx-client

# initialise database and make updates if necessary
python -m diracx.db init-sql

exec "$@"
```

## Install the code

```bash
git clone https://github.com/DIRACGrid/diracx.git
git checkout v0.0.12
```

## Tests

Once the containers are installed, the following tests should be working (note: when testing, we needed to run the
first command twice as there seemed to be a bug in DiracX. This might be fixed by the time you are trying this.)
We ran these as root, as we were setting up our apache config as root and debugging this config as part of the process, but you can run this as any user.

```
[root@diractest ~]# curl -k https://diractest.grid.hep.ph.ic.ac.uk/.well-known/openid-configuration`
{"issuer":"https://diractest.grid.hep.ph.ic.ac.uk","token_endpoint":"https://diractest.grid.hep.ph.ic.ac.uk/api/auth/token","userinfo_endpoint":"https://diractest.grid.hep.ph.ic.ac.uk/api/auth/userinfo","authorization_endpoint":
[etc]
```

```
[root@diractest ~]#  curl -k https://diractest.grid.hep.ph.ic.ac.uk/.well-known/jwks.json`
{"keys":[{"crv":"Ed25519","x":"3_SrPXQyalji7nL4fNFh7JGBTwQBztmjnW7ogFusiPs","key_ops":["sign","verify"],"alg":"EdDSA","kid":"019a3a1b95b77b11a18bde7813b78fe2","kty":"OKP"}]}
```

To test the web container:

```bash
$ curl -k https://diractest.grid.hep.ph.ic.ac.uk/
<!DOCTYPE html><html lang="en"><head>[etc] (i.e. returns a web site)
```

## A selection of helpful podman commands

```
podman ps -a # see all containers, dead or alive
podman rm diracx-container_diracx-services_1 # remove dead container by name
podman-compose -f podman-compose.yaml up -d --force-recreate # restart all containers (e.g. after update)
podman exec -ti diracx-container_diracx-services_1 /bin/bash # to look into a running container
podman logs -f diracx-container_diracx-services_1 # to look at the logs of a running container
# clean up
podman images # list all images
podman image prune # should delete all unused images
podman image rm [image id] # in case the automated delete doesn't work
```
