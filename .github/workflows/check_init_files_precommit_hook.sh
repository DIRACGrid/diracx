#!/bin/sh
# Obtain the files that don't match
result=$(grep -L '__all__ =' "$@")

# If no result, hook ok
if [ -z "$result" ]; then
    exit 0
fi

# Else, show which files
echo "$result"
exit 1
