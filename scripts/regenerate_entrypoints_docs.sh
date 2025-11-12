#!/usr/bin/env bash
# Convenience script to regenerate entry points documentation
# Usage: ./scripts/regenerate_entrypoints_docs.sh

set -euo pipefail

cd "$(dirname "$0")/.."

echo "Regenerating entry points documentation..."
pixi run -e default python scripts/generate_entrypoints_docs.py

echo ""
echo "✓ Documentation updated at docs/dev/reference/entrypoints.md"
echo ""
echo "To view changes:"
echo "  git diff docs/dev/reference/entrypoints.md"
