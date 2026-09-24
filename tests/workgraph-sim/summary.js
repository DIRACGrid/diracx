// Wait for every suite, print the result, and fail the process if any check failed.
(function (g) {
  const { out, state } = g.WG_TEST;

  const report = () => {
    for (const name of state.open) {
      state.failures += 1;
      out(`  FAIL ${name}.test.js never finished`);
    }
    out(`${state.checks} checks, ${state.failures} failure(s)`);
    if (!state.failures) return;
    if (typeof process !== 'undefined') process.exitCode = 1;
    else throw new Error(`${state.failures} workgraph simulator check(s) failed`);
  };

  let settled = false;
  const done = Promise.all(state.suites).then(
    () => (settled = true),
    (e) => {
      settled = true;
      state.failures += 1;
      out(`  FAIL a suite rejected: ${(e && e.stack) || e}`);
    }
  );

  /* The jsc shell ends the script before its promises settle, so the checks are pumped here.
     `setTimeout` is synchronous under that shell (see elk-shim.js), so draining is enough.
     Reporting happens after the loop, never inside it, or a throw would look like a hang. */
  if (typeof drainMicrotasks === 'function') {
    for (let i = 0; i < 200000 && !settled; i++) drainMicrotasks();
    if (!settled) {
      state.failures += 1;
      out('  FAIL the checks did not settle');
    }
    report();
  } else {
    g.WG_TEST.done = done.then(report);
  }
})(globalThis);
