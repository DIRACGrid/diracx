// Node entry point: load elkjs, the simulator, then the checks, sharing one global scope.
// Usage: node tests/workgraph-sim/run.js <documentation pages with workgraph fences...>
const path = require('path');
const root = path.resolve(__dirname, '..', '..');
for (const file of [
  'tests/workgraph-sim/elk-shim.js',
  'docs/assets/js/workgraph-sim-engine.js',
  'docs/assets/js/workgraph-sim.js',
  'docs/assets/js/workgraph-cwl.js',
  'tests/workgraph-sim/harness.js',
  'tests/workgraph-sim/engine.test.js',
  'tests/workgraph-sim/invariants.test.js',
  'tests/workgraph-sim/features.test.js',
  'tests/workgraph-sim/controls.test.js',
  'tests/workgraph-sim/help.test.js',
  'tests/workgraph-sim/cwl.test.js',
  'tests/workgraph-sim/docs.test.js',
  'tests/workgraph-sim/summary.js',
]) {
  require(path.join(root, file));
}
globalThis.WG_TEST.done.catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
