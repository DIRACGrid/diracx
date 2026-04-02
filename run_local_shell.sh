#!/usr/bin/env bash
# Open a shell with the local DiracX environment configured
set -euo pipefail

env_file_pointer="$(cd "$(dirname "$0")" && pwd)/.run-local-env"

if [ ! -f "$env_file_pointer" ]; then
  echo "❌ No running local-start instance found"
  echo "   Start one first: pixi run local-start"
  exit 1
fi

env_file=$(cat "$env_file_pointer")

if [ ! -f "$env_file" ]; then
  echo "❌ env.sh not found at $env_file (local-start may have stopped)"
  exit 1
fi

# Source the env (includes the CONDA_PREFIX guard)
# shellcheck disable=SC1090
source "$env_file"

# Generate a token
keystore="${DIRACX_SERVICE_AUTH_TOKEN_KEYSTORE#file://}"
python tests/make_token_local.py "$keystore"

echo "🐚 DiracX local shell — type 'exit' to return"
exec bash --norc --noprofile
