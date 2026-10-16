#!/usr/bin/env bash
# Run the workgraph simulator checks with node, or with macOS's JavaScriptCore shell when node is absent.
set -euo pipefail
cd "$(dirname "$0")/../.."

pages=()
while IFS= read -r page; do
    pages+=("$page")
done < <(grep -rlE '^ *```workgraph' docs --include='*.md' | sort)

# The layout engine comes from the environment rather than from the tree. This script is run
# both as `pixi run test-workgraph-sim`, where CONDA_PREFIX is set, and straight from the
# repository, where it is not; and jsc has no process.env, so the path is worked out here and
# passed to both shells. Resolved once, so a missing engine says so instead of surfacing as
# `Could not open file` from inside a shim.
elk=""
for prefix in "${CONDA_PREFIX:-}" .pixi/envs/workgraph-sim; do
    if [ -n "$prefix" ] && [ -f "$prefix/lib/node_modules/elkjs/lib/elk.bundled.js" ]; then
        elk="$prefix/lib/node_modules/elkjs/lib/elk.bundled.js"
        break
    fi
done
if [ -z "$elk" ]; then
    echo "elkjs is not in this environment: run 'pixi install -e workgraph-sim'" >&2
    exit 1
fi

if command -v node >/dev/null 2>&1; then
    exec node tests/workgraph-sim/run.js "--elk=$elk" "${pages[@]}"
fi

jsc=/System/Library/Frameworks/JavaScriptCore.framework/Versions/Current/Helpers/jsc
if [ -x "$jsc" ]; then
    exec "$jsc" \
        tests/workgraph-sim/elk-shim.js \
        docs/assets/js/workgraph-sim-engine.js \
        docs/assets/js/workgraph-sim.js \
        docs/assets/js/workgraph-cwl.js \
        tests/workgraph-sim/harness.js \
        tests/workgraph-sim/engine.test.js \
        tests/workgraph-sim/invariants.test.js \
        tests/workgraph-sim/features.test.js \
        tests/workgraph-sim/controls.test.js \
        tests/workgraph-sim/help.test.js \
        tests/workgraph-sim/cwl.test.js \
        tests/workgraph-sim/docs.test.js \
        tests/workgraph-sim/summary.js \
        -- "--elk=$elk" "${pages[@]}"
fi

echo "neither node nor jsc is available" >&2
exit 1
