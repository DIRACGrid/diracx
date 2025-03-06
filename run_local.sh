#!/usr/bin/env bash
# Run a local uvicorn server with the default configuration
set -euo pipefail
IFS=$'\n\t'

tmp_dir=$(mktemp -d)
echo "Using temp dir: ${tmp_dir}"
mkdir -p "${tmp_dir}/signing-key" "${tmp_dir}/cs_store/"

signing_key="${tmp_dir}/signing-key/rsa256.key"
ssh-keygen -P "" -t rsa -b 4096 -m PEM -f "${signing_key}"

state_key="$(head -c 32 /dev/urandom | base64)"

# Make a fake CS
dirac internal generate-cs "${tmp_dir}/cs_store/initialRepo"

dirac internal add-vo "${tmp_dir}/cs_store/initialRepo" \
    --vo=diracAdmin \
    --idp-url=runlocal.diracx.invalid \
    --idp-client-id="idp-client-id" \
    --default-group=admin

dirac internal add-user "${tmp_dir}/cs_store/initialRepo" \
  --vo=diracAdmin --group=admin \
  --sub=75212b23-14c2-47be-9374-eb0113b0575e \
  --preferred-username=localuser

export DIRACX_CONFIG_BACKEND_URL="git+file://${tmp_dir}/cs_store/initialRepo"
export DIRACX_DB_URL_AUTHDB="sqlite+aiosqlite:///${tmp_dir}/authdb.db"
export DIRACX_DB_URL_JOBDB="sqlite+aiosqlite:///${tmp_dir}/jobdb.db"
export DIRACX_DB_URL_JOBLOGGINGDB="sqlite+aiosqlite:///${tmp_dir}/jobloggingdb.db"
export DIRACX_DB_URL_SANDBOXMETADATADB="sqlite+aiosqlite:///${tmp_dir}/sandboxmetadatadb.db"
export DIRACX_DB_URL_TASKQUEUEDB="sqlite+aiosqlite:///${tmp_dir}/taskqueuedb.db"
# This script monkey patches the parameter db to use a sqlite database rather
# than requiring a full opensearch instance so we use a sqlalchmey dsn here
export DIRACX_OS_DB_JOBPARAMETERSDB='{"sqlalchemy_dsn": "sqlite+aiosqlite:///'${tmp_dir}'/jobparametersdb.db"}'
export DIRACX_SERVICE_AUTH_TOKEN_KEY="file://${signing_key}"
export DIRACX_SERVICE_AUTH_STATE_KEY="${state_key}"
hostname_lower=$(hostname | tr -s '[:upper:]' '[:lower:]')
export DIRACX_SERVICE_AUTH_TOKEN_ISSUER="http://${hostname_lower}:8000"
export DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS='["http://'"$hostname_lower"':8000/docs/oauth2-redirect"]'
export DIRACX_SANDBOX_STORE_BUCKET_NAME=sandboxes
export DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET=true
export DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS='{"endpoint_url": "http://localhost:3000", "aws_access_key_id": "console", "aws_secret_access_key": "console123"}'

moto_server -p3000 &
moto_pid=$!
uvicorn --factory diracx.testing.routers:create_app --reload &
diracx_pid=$!

success=0
for _ in {1..10}; do
  if curl --silent --head http://localhost:8000 > /dev/null; then
    success=1
    break
  fi
  sleep 1
done

echo ""
echo ""
echo ""
if [ $success -eq 0 ]; then
  echo "Failed to start DiracX"
else
  echo "DiracX is running on http://localhost:8000"
fi
echo "To interact with DiracX you can:"
echo ""
echo "1. Use the CLI:"
echo ""
echo "    export DIRACX_URL=http://localhost:8000"
echo "    env DIRACX_SERVICE_AUTH_STATE_KEY='${state_key}' tests/make_token_local.py ${signing_key}"
echo ""
echo "2. Using swagger: http://localhost:8000/api/docs"

function cleanup(){
  trap - SIGTERM
  kill $moto_pid
  kill $diracx_pid
  echo "Waiting for proccesses to exit"
  wait $moto_pid $diracx_pid
  echo "Cleaning up"
  rm -rf "${tmp_dir}"
}

trap "cleanup" EXIT

while true; do
  sleep 1
done
