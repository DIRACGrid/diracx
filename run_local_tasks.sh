#!/usr/bin/env bash
# Run diracx-task-run with the local environment variables
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

# shellcheck disable=SC1090
source "$env_file"

exec diracx-task-run "$@"
