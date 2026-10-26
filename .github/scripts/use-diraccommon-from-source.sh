#!/usr/bin/env bash
# Replace the diraccommon release from PyPI with dirac-common from a DIRAC checkout
# Usage: use-diraccommon-from-source.sh <dirac_dir>
#   dirac_dir: Path to a DIRAC git checkout
# Must be run from the root of the diracx repository, with pixi available.
# Updates pixi.toml, pixi.lock and containers/Dockerfile in place.

set -euo pipefail

DIRAC_DIR="${1:?Usage: $0 <dirac_dir>}"
DEST=DIRACCommon

rm -rf "${DEST}"
cp -r "${DIRAC_DIR}/dirac-common" "${DEST}"
cp "${DIRAC_DIR}/LICENSE" "${DEST}/LICENSE"

# dirac-common takes its version and LICENSE from the parent DIRAC repository,
# make it standalone so it can be built from inside the container build context
sed -i 's@^root = "\.\."@root = "."@; s@"\.\./LICENSE"@"LICENSE"@' "${DEST}/pyproject.toml"

# diracx requires a minimum diraccommon version so give it one which is always newer
git -C "${DEST}" init -q
git -C "${DEST}" add -A
git -C "${DEST}" -c user.name="DIRACGrid CI" -c user.email="ci@diracgrid.org" -c commit.gpgsign=false \
  commit -q -m "dirac-common from DIRAC $(git -C "${DIRAC_DIR}" rev-parse HEAD)"
git -C "${DEST}" tag 99.99.99

sed -i '/^\[feature\.diracx-core\.pypi-dependencies\]$/a diraccommon = { path = "DIRACCommon", editable = false }' pixi.toml
sed -i '/^COPY pyproject.toml/i COPY DIRACCommon/ DIRACCommon/' containers/Dockerfile

pixi lock

git diff --stat
git diff pixi.toml containers/Dockerfile
