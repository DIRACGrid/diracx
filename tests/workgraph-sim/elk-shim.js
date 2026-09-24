// Load elkjs under whichever shell is running the checks.
//
// The bundle itself comes from the environment rather than the tree; `run.sh` works the path out
// once and passes it in. The macOS JavaScriptCore shell needs a small browser shim for the
// GWT-compiled bundle, and a synchronous setTimeout: elkjs resolves its layout through a
// timer, and the jsc shell only drains its own timer queue when the script has ended, which
// is after summary.js has already reported.
(function (g) {
  /* `run.sh` resolves the bundle out of the environment and passes it as `--elk=<path>`, since jsc
     has no `process.env` to read `CONDA_PREFIX` from and no way to be given one. Under node the
     argument is there too, and the environment is the fallback for a run that bypassed the script. */
  const argv = typeof process !== 'undefined' && process.argv ? process.argv : Array.from(g.arguments || []);
  const flag = argv.find((a) => typeof a === 'string' && a.startsWith('--elk='));
  const given = flag ? flag.slice('--elk='.length) : null;
  const inPrefix = typeof process !== 'undefined' && process.env && process.env.CONDA_PREFIX
    ? process.env.CONDA_PREFIX + '/lib/node_modules/elkjs/lib/elk.bundled.js'
    : null;
  const bundle = given || inPrefix;
  if (!bundle) throw new Error("elkjs is not in this environment: run 'pixi install -e workgraph-sim'");
  if (typeof require === 'function') {
    g.ELK = require(require('path').resolve(bundle));
    return;
  }
  g.window = g;
  g.$wnd = g;
  g.Error.stackTraceLimit = 64;
  g.setTimeout = function (f) {
    f();
    return 0;
  };
  g.clearTimeout = function () {};
  /* elkjs wants a console; it must really print, or the checks would report nothing and still pass */
  g.console = g.console || {};
  const say = function () {
    print(Array.prototype.join.call(arguments, ' '));
  };
  for (const k of ['log', 'err', 'warn', 'info', 'error', 'debug']) if (!g.console[k]) g.console[k] = say;
  const module = { exports: {} };
  g.module = module;
  g.exports = module.exports;
  load(bundle);
  g.ELK = module.exports;
  delete g.module;
  delete g.exports;
})(globalThis);
