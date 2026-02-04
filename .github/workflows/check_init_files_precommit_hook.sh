#!/bin/bash
# Obtain the files that don't match
result=$(find ./diracx-* -type f -name '__init__.py' -exec grep -L '__all__ =' {} +)

# If no result, hook ok
if [ -z result ]; then
    exit 0
fi

# Else, show which files
echo The following files do not have __all__ defined in them:
echo "$result"
exit 1
