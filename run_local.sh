#!/usr/bin/env bash

### Runs a local uvicorn server with the default configuration


set -euo pipefail
IFS=$'\n\t'

tmp_dir=$(mktemp -d)
mkdir -p "${tmp_dir}/signing-key" "${tmp_dir}/cs_store/"

ssh-keygen -P "" -t rsa -b 4096 -m PEM -f "${tmp_dir}/signing-key/rs256.key"

dirac internal generate-cs "${tmp_dir}/cs_store/initialRepo" --vo=diracAdmin --user-group=admin --idp-url=runlocal.diracx.invalid

export DIRACX_CONFIG_BACKEND_URL="git+file://${tmp_dir}/cs_store/initialRepo"
export DIRACX_DB_URL_AUTHDB="sqlite+aiosqlite:///:memory:"
export DIRACX_DB_URL_JOBDB="sqlite+aiosqlite:///:memory:"
export DIRACX_SERVICE_AUTH_TOKEN_KEY="file://${tmp_dir}/signing-key/rs256.key"
export DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS='["http://'$(hostname| tr -s '[:upper:]' '[:lower:]')':8000/docs/oauth2-redirect"]'


uvicorn --factory diracx.routers:create_app --reload

function cleanup(){
  trap - SIGTERM;
  echo "Cleaning up";
  rm -rf "${tmp_dir}"
}

trap "cleanup" EXIT
