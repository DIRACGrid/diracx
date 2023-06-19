#!/bin/bash

ulimit -n 8192
eval "$(micromamba shell hook --shell=posix)" && micromamba activate base
exec "$@"
