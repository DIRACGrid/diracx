// Shared helpers for the workgraph simulator checks. Runs under node (via run.js) or the jsc shell (via run.sh).
(function (g) {
  const out = typeof console !== 'undefined' && console.log ? (s) => console.log(s) : g.print;
  const { Sim, DEFAULT_SETTINGS } = g.WorkgraphSimEngine;
  /* Every model these checks build verifies itself after each step: inbound minus outbound against the occupancy, per input
     state. The engine leaves it off, since it costs a pass over every input on every step of every page. */
  DEFAULT_SETTINGS.verify = true;
  const state = { failures: 0, checks: 0, open: [], suites: [] };

  /* The step ceiling for a check that a workgraph terminates. 200000 steps is 10000 model seconds, some forty times what the
     longest model the documentation ships needs, so a run only reaches it by hanging. A budget fitted to what a run takes
     today fails the moment the data shifts, which says nothing about the model; termination is what these checks are about,
     and the ceiling exists only so a hang reports rather than spins. Reaching it costs under two seconds on the largest
     model, which is the price of a failure and nothing at all otherwise. */
  const HANG = 200000;

  /* Each file's body is asynchronous, because laying a model out is. A file registers its promise
     here and summary.js waits for them all, which is what keeps the report after the checks. */
  function suite(name, body) {
    begin(name);
    state.suites.push(
      Promise.resolve()
        .then(body)
        .then(() => end(name))
        .catch((e) => {
          state.failures += 1;
          out(`  FAIL ${name} threw: ${(e && e.stack) || e}`);
          end(name);
        })
    );
  }

  /* A file that throws stops loading, and the checks after it never run. Each file brackets itself so the summary can say so. */
  function begin(name) {
    state.open.push(name);
  }

  function end(name) {
    const k = state.open.indexOf(name);
    if (k >= 0) state.open.splice(k, 1);
  }

  function check(cond, message) {
    state.checks += 1;
    if (!cond) {
      state.failures += 1;
      out('  FAIL ' + message);
    }
  }

  /* Step the model until the predicate holds or the step budget runs out. */
  function until(sim, pred, limit) {
    let n = 0;
    while (!pred() && n < (limit || 20000)) {
      sim.step(0.05);
      n++;
    }
    return pred();
  }

  /* A started model whose manual sign-offs pass by themselves. */
  function started(spec, extra) {
    const sim = new Sim(Object.assign({}, spec, { workgraph: Object.assign({}, spec.workgraph || {}, { approval: 'auto' }, extra || {}) }));
    sim.start();
    return sim;
  }

  /* What an operator does once a member has nothing left but its quarantine: write the Problematic inputs off, so the
     member can drain (DX-ADR-005). Returns the ids written off. */
  function writeOffQuarantine(sim) {
    const done = [];
    if (sim.wg.status !== 'Active' && sim.wg.status !== 'Scouting') return done;
    for (const n of Object.values(sim.nodes)) {
      if (!sim.waitsOnOperator(n)) continue;
      sim.writeOffProblematic(n.id);
      done.push(n.id);
    }
    return done;
  }

  /* What an operator does for a member held back for them: press its start button (DX-ADR-005). A member held for an operator
     waits for a person and drains for nobody, so a model with one never reaches Completed by itself; the harness is that
     person. Only an `operator` hold: an `approval` hold is released when the workgraph is approved, and pressing start in its
     place would run it through the scout it is held back for. Gated on `Active`, the one state the button is offered in.
     Returns the ids started. */
  function startHeldMembers(sim) {
    const done = [];
    if (sim.wg.status !== 'Active') return done;
    for (const n of Object.values(sim.nodes)) {
      if (n.status !== 'Paused' || n.spec.hold !== 'operator') continue;
      sim.startNode(n.id);
      done.push(n.id);
    }
    return done;
  }

  /* `until`, with an operator writing off each quarantine as soon as it is all that holds a member: for a run that has to
     drain past the odd input its hook gave up on. Not for a check on the hold itself, which uses `until`. */
  function drain(sim, pred, limit) {
    let n = 0;
    while (!pred() && n < (limit || 20000)) {
      sim.step(0.05);
      writeOffQuarantine(sim);
      n++;
    }
    return pred();
  }

  /* Run a started model to Completed, forcing blocked actions, writing off quarantines and starting the members held back for
     an operator, as an operator would. */
  function complete(sim, limit) {
    let steps = 0;
    while (sim.wg.status !== 'Completed' && steps < (limit || HANG)) {
      sim.step(0.05);
      steps++;
      if (sim.blocked) sim.forceAction();
      writeOffQuarantine(sim);
      startHeldMembers(sim);
    }
    return steps * 0.05;
  }

  function readText(path) {
    if (typeof require === 'function') return require('fs').readFileSync(path, 'utf8');
    return g.readFile(path);
  }

  /* Files named on the command line: node's argv, or the jsc shell's `arguments` after `--`. The
     runner passes the layout engine's path the same way, `--elk=<path>` for the shim to read, so
     anything that begins with a dash is a setting rather than a page. */
  function fileArgs() {
    const argv = typeof process !== 'undefined' && process.argv ? process.argv.slice(2) : typeof g.arguments !== 'undefined' ? Array.from(g.arguments) : [];
    return argv.filter((a) => !String(a).startsWith('--'));
  }

  g.WG_TEST = { out, check, until, drain, started, complete, writeOffQuarantine, startHeldMembers, readText, fileArgs, begin, end, suite, state, HANG };
})(globalThis);
