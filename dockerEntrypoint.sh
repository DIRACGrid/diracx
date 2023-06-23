#!/bin/bash

eval "$(micromamba shell hook --shell=posix)" && micromamba activate base

echo "Running uvicorn with extra options $@"
exec uvicorn --factory diracx.routers:create_app --host 0.0.0.0 --port 8000 "$@"
