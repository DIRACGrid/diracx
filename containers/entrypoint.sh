#!/bin/bash
set -e

source /activate.sh
if [ -f /pre-run-hook.sh ]; then
    source /pre-run-hook.sh
fi

exec "$@"
