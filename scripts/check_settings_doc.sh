#!/usr/bin/env bash
set -euo pipefail

# Run the settings-doc generator via pixi and fail if the committed file would change.
# This allows a pre-commit hook to check that generated documentation is up-to-date
# without letting the hook itself modify the repository and cause the hook to fail.

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

TARGET="docs/dev/reference/env-variables.md"

# Save the current state of the target file
cp "$TARGET" "${TARGET}.backup"

echo "Running settings-doc generator (pixi run generate-settings-doc)..."
pixi run generate-settings-doc

# Normalize both files by removing empty lines, then compare
# This handles cases where settings-doc adds/removes blank lines inconsistently
normalize_file() {
    grep -v '^[[:space:]]*$' "$1" || true
}

if diff -q <(normalize_file "$TARGET") <(normalize_file "${TARGET}.backup") > /dev/null 2>&1; then
  # Files are equivalent ignoring blank lines, restore original and pass
  cp "${TARGET}.backup" "$TARGET"
  rm "${TARGET}.backup"
  echo "Generated documentation is up-to-date."
  exit 0
fi

# Files have meaningful differences
echo
echo "ERROR: Generated settings documentation is out of date."
echo "Run 'pixi run generate-settings-doc' and commit the updated file."
echo
echo "Diff (generated vs current):"
diff "$TARGET" "${TARGET}.backup" || true
# Restore the original file so pre-commit doesn't see modifications
mv "${TARGET}.backup" "$TARGET"
exit 1
