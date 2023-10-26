#!/usr/bin/env bash
# Run a local uvicorn server with the default configuration
set -euo pipefail
IFS=$'\n\t'

tmp_dir=$(mktemp -d)
echo "Using temp dir: ${tmp_dir}"
mkdir -p "${tmp_dir}/signing-key" "${tmp_dir}/cs_store/"

signing_key="${tmp_dir}/signing-key/rsa256.key"
ssh-keygen -P "" -t rsa -b 4096 -m PEM -f "${signing_key}"

# Make a fake CS
dirac internal generate-cs "${tmp_dir}/cs_store/initialRepo" \
  --vo=diracAdmin --user-group=admin --idp-url=runlocal.diracx.invalid
dirac internal add-user "${tmp_dir}/cs_store/initialRepo" \
  --vo=diracAdmin --user-group=admin \
  --sub=75212b23-14c2-47be-9374-eb0113b0575e \
  --preferred-username=localuser

export DIRACX_CONFIG_BACKEND_URL="git+file://${tmp_dir}/cs_store/initialRepo"
export DIRACX_DB_URL_AUTHDB="sqlite+aiosqlite:///:memory:"
export DIRACX_DB_URL_JOBDB="sqlite+aiosqlite:///:memory:"
export DIRACX_DB_URL_JOBLOGGINGDB="sqlite+aiosqlite:///:memory:"
export DIRACX_DB_URL_SANDBOXMETADATADB="sqlite+aiosqlite:///:memory:"
export DIRACX_DB_URL_TASKQUEUEDB="sqlite+aiosqlite:///:memory:"
export DIRACX_SERVICE_AUTH_TOKEN_KEY="file://${signing_key}"
export DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS='["http://'$(hostname| tr -s '[:upper:]' '[:lower:]')':8000/docs/oauth2-redirect"]'
export DIRACX_SANDBOX_STORE_BUCKET_NAME=sandboxes
export DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET=true
export DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS='{"endpoint_url": "http://localhost:3000", "aws_access_key_id": "console", "aws_secret_access_key": "console123"}'

moto_server -p3000 &
moto_pid=$!
uvicorn --factory diracx.routers:create_app --reload &
diracx_pid=$!

success=0
for i in {1..10}; do
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
echo "    tests/make-token-local.py ${signing_key}"
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
